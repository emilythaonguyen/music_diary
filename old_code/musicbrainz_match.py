import asyncio
import asyncpg
import musicbrainzngs
import functools
from tqdm import tqdm
from dotenv import load_dotenv
import os
import re
import unicodedata
from rapidfuzz import fuzz
from unidecode import unidecode
import pykakasi
from korean_romanizer.romanizer import Romanizer
from pypinyin import lazy_pinyin

_kks = pykakasi.kakasi()
load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================

DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

mb_user_agent = os.getenv("MB_USER_AGENT")
if not mb_user_agent:
    raise ValueError("MB_USER_AGENT environment variable is missing!")
musicbrainzngs.set_useragent("ListenBrainzMatcher", "1.0", mb_user_agent)

RATE_LIMIT_DELAY = 1.1

# Schema relationships
FK_RELATIONSHIPS = {
    "artists": [
        "artist_aliases", "artist_genres", "listens", 
        "release_artists", "spotify_artist_popularity", "releases"
    ],
    "releases": [
        "listens", "release_artists", "release_genres", 
        "spotify_release_popularity", "tracks"
    ],
    "tracks": [
        "listens", "spotify_track_popularity", 
        "track_artists", "track_audio_features"
    ]
}

ENTITY_CONFIG = {
    "artists": {
        "pk": "artist_id",
        "mbid": "artist_mbid",
        "name": "artist_name",
        "mb_search": musicbrainzngs.search_artists,
        "mb_get": musicbrainzngs.get_artist_by_id,
        "mb_name_field": "name",
        "similarity_threshold": 0.9,
        "has_release_group": False
    },
    "releases": {
        "pk": "release_id",
        "mbid": "release_mbid",
        "name": "release_name",
        "mb_search": musicbrainzngs.search_releases,
        "mb_get": musicbrainzngs.get_release_by_id,
        "mb_name_field": "title",
        "similarity_threshold": 0.8,
        "has_release_group": True
    }
}

# ============================================================
# TEXT NORMALIZATION & SIMILARITY
# ============================================================

def normalize_name(name: str) -> str:
    """Normalize text for comparison by removing versioning/remaster info."""
    if not name:
        return ""

    name = unicodedata.normalize("NFKC", name).strip().lower()
    
    cruft = (
        r"remaster(ed)?|version|mix|edit|live|mono|stereo|bonus|session|take|"
        r"radio|acoustic|original|alternate|demo|instrumental|explicit"
    )
    
    # Remove trailing years and cruft
    name = re.sub(r"[\s]*[-–—:]+\s*\d{4}(\s+(" + cruft + r"))?.*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\(\s*\d{4}(\s+(" + cruft + r"))?\s*\)$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"[\s]*[-–—:]+\s*(" + cruft + r")\b.*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\((\s*" + cruft + r"\s*)\)$", "", name, flags=re.IGNORECASE)
    
    # Remove features
    name = re.sub(r"[\s\(\[]+(feat|ft|featuring|w\/|with)\s+.+?[\)\]]?$", "", name, flags=re.IGNORECASE)
    
    # Clean punctuation
    name = re.sub(r"[^\w\s#\-]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    
    return name

def detect_script(text: str) -> str:
    """Detect if text is Japanese, Korean, Chinese, or Latin."""
    for ch in text:
        code = ord(ch)
        if 0xAC00 <= code <= 0xD7AF or 0x1100 <= code <= 0x11FF:
            return "ko"
        if 0x3040 <= code <= 0x30FF:
            return "ja"
        if 0x4E00 <= code <= 0x9FFF:
            return "zh"
    return "latin"

def transliterate(text: str) -> str:
    """Transliterate non-Latin scripts to Latin."""
    if not text:
        return ""
    
    script = detect_script(text)
    
    try:
        if script == "ja":
            result = _kks.convert(text)
            return " ".join(item['hepburn'] for item in result)
        if script == "ko":
            return normalize_name(Romanizer(text).romanize())
        if script == "zh":
            return normalize_name(" ".join(lazy_pinyin(text)))
    except Exception:
        pass
    
    return normalize_name(unidecode(text))

def similarity_score(a: str, b: str) -> float:
    """Calculate similarity between two strings (0-1 scale)."""
    a_norm = normalize_name(a)
    b_norm = normalize_name(b)
    
    set_score = fuzz.token_set_ratio(a_norm, b_norm)
    partial = fuzz.partial_ratio(a_norm, b_norm)
    
    return (0.6 * set_score + 0.4 * partial) / 100

# ============================================================
# MUSICBRAINZ API HELPERS
# ============================================================

async def safe_mb_call(func, *args, retries=5, delay=2, **kwargs):
    """Async wrapper for blocking MusicBrainz calls with retry logic."""
    loop = asyncio.get_event_loop()
    for attempt in range(retries):
        try:
            return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))
        except Exception as e:
            print(f"❗️ MB request failed: {e}. Retry {attempt + 1}/{retries} in {delay}s")
            await asyncio.sleep(delay)
            delay *= 2
    print(f"❌ MB request failed after {retries} attempts")
    return None

def extract_spotify_id(url_relations: list) -> str:
    """Extract Spotify ID from MusicBrainz URL relations."""
    for rel in url_relations:
        target = rel.get("target", "")
        if "spotify.com/" in target:
            return target.split("/")[-1].split("?")[0]
    return None

async def fetch_mb_entity_details(entity_type: str, mbid: str):
    """Fetch full entity details from MusicBrainz."""
    config = ENTITY_CONFIG[entity_type]
    includes = ["url-rels", "release-groups"] if config["has_release_group"] else ["url-rels"]
    
    result = await safe_mb_call(config["mb_get"], mbid, includes=includes)
    await asyncio.sleep(RATE_LIMIT_DELAY)
    
    if result and entity_type[:-1] in result:
        return result[entity_type[:-1]]
    return None

# ============================================================
# DATABASE MERGE LOGIC
# ============================================================

async def merge_entity(pool, entity_type: str, canonical_id: int, duplicate_id: int):
    """
    Merge duplicate entity into canonical one by updating all foreign keys.
    
    Args:
        pool: Database connection pool
        entity_type: "artists", "releases", or "tracks"
        canonical_id: ID to keep
        duplicate_id: ID to merge and delete
    """
    config = ENTITY_CONFIG.get(entity_type)
    if not config:
        print(f"❌ Unknown entity type: {entity_type}")
        return False
    
    pk_column = config["pk"]
    fk_column = pk_column  # Foreign key column name matches primary key
    
    # Update all foreign key references
    for ref_table in FK_RELATIONSHIPS.get(entity_type, []):
        try:
            # Handle special case where FK column name differs
            if entity_type == "artists" and ref_table == "releases":
                fk_column = "primary_artist_id"
            else:
                fk_column = pk_column
            
            await pool.execute(
                f"UPDATE {ref_table} SET {fk_column} = $1 WHERE {fk_column} = $2",
                canonical_id,
                duplicate_id
            )
        except Exception as e:
            print(f"⚠️  Warning: Could not update {ref_table}.{fk_column}: {e}")
    
    # Delete the duplicate
    try:
        await pool.execute(
            f"DELETE FROM {entity_type} WHERE {pk_column} = $1",
            duplicate_id
        )
        print(f"🟢 Merged {entity_type[:-1]} {duplicate_id} → {canonical_id}")
        return True
    except Exception as e:
        print(f"❌ Failed to delete duplicate {entity_type[:-1]} {duplicate_id}: {e}")
        return False

async def assign_mbid_with_merge(pool, entity_type: str, local_id: int, mbid: str):
    """
    Assign MBID to entity. If MBID already exists, merge the duplicate.
    
    Returns:
        int: The final ID (may be different if merged)
    """
    config = ENTITY_CONFIG[entity_type]
    pk_column = config["pk"]
    mbid_column = config["mbid"]
    
    # Check if MBID already exists
    existing = await pool.fetchrow(
        f"SELECT {pk_column} FROM {entity_type} WHERE {mbid_column} = $1 AND {pk_column} != $2",
        mbid,
        local_id
    )
    
    if existing:
        # Merge into existing entity
        canonical_id = existing[pk_column]
        await merge_entity(pool, entity_type, canonical_id, local_id)
        return canonical_id
    
    # No conflict - assign MBID
    await pool.execute(
        f"UPDATE {entity_type} SET {mbid_column} = $1 WHERE {pk_column} = $2",
        mbid,
        local_id
    )
    return local_id

# ============================================================
# MBID ASSIGNMENT
# ============================================================

async def match_and_assign_mbid(
    pool,
    entity_type: str,
    local_id: int,
    local_name: str,
    search_results: list,
    local_spotify_id: str = None,
    extra_search_params: dict = None
) -> str:
    """
    Match local entity to MusicBrainz and assign MBID.
    
    Returns:
        str: Assigned MBID or None
    """
    config = ENTITY_CONFIG[entity_type]
    
    # ===============================================
    # PASS 1: Try exact Spotify ID match
    # ===============================================
    for candidate in search_results:
        mb_entity = await fetch_mb_entity_details(entity_type, candidate["id"])
        if not mb_entity:
            continue
        
        mbid = mb_entity["id"]
        mb_spotify_id = extract_spotify_id(mb_entity.get("url-relation-list", []))
        
        # Check Spotify match
        if local_spotify_id and mb_spotify_id and local_spotify_id == mb_spotify_id:
            # Fill missing local Spotify ID
            if not local_spotify_id:
                await pool.execute(
                    f"UPDATE {entity_type} SET spotify_id = $1 WHERE {config['pk']} = $2",
                    mb_spotify_id,
                    local_id
                )
            
            # Assign MBID (with merge if duplicate)
            final_id = await assign_mbid_with_merge(pool, entity_type, local_id, mbid)
            
            # Handle release group for releases
            if config["has_release_group"]:
                rg_mbid = mb_entity.get("release-group", {}).get("id")
                if rg_mbid:
                    await pool.execute(
                        "UPDATE releases SET release_group_mbid = $1 WHERE release_id = $2",
                        rg_mbid,
                        final_id
                    )
            
            print(f"🟢 {entity_type[:-1].capitalize()} '{local_name}' → MBID {mbid} (Spotify match)")
            return mbid
    
    # ===============================================
    # PASS 2: Best name similarity match
    # ===============================================
    best_candidate = max(
        search_results,
        key=lambda c: similarity_score(local_name, c.get(config["mb_name_field"], ""))
    )
    
    score = similarity_score(local_name, best_candidate.get(config["mb_name_field"], ""))
    
    if score >= config["similarity_threshold"]:
        mbid = best_candidate["id"]
        mb_entity = await fetch_mb_entity_details(entity_type, mbid)
        
        if mb_entity:
            mb_spotify_id = extract_spotify_id(mb_entity.get("url-relation-list", []))
            
            # Fill missing Spotify ID
            if not local_spotify_id and mb_spotify_id:
                await pool.execute(
                    f"UPDATE {entity_type} SET spotify_id = $1 WHERE {config['pk']} = $2",
                    mb_spotify_id,
                    local_id
                )
            
            # Assign MBID
            final_id = await assign_mbid_with_merge(pool, entity_type, local_id, mbid)
            
            # Handle release group
            if config["has_release_group"]:
                rg_mbid = mb_entity.get("release-group", {}).get("id")
                if rg_mbid:
                    await pool.execute(
                        "UPDATE releases SET release_group_mbid = $1 WHERE release_id = $2",
                        rg_mbid,
                        final_id
                    )
            
            print(f"🟢 {entity_type[:-1].capitalize()} '{local_name}' → MBID {mbid} (similarity={score:.2f})")
            return mbid
    
    print(f"🔴 No match for {entity_type[:-1]} '{local_name}' (best score={score:.2f})")
    return None

# ============================================================
# TRACK MATCHING
# ============================================================

async def fetch_release_tracks(release_mbid: str) -> list:
    """Fetch all tracks/recordings from a release."""
    data = await safe_mb_call(
        musicbrainzngs.get_release_by_id,
        release_mbid,
        includes=["recordings"]
    )
    
    if not data or "release" not in data:
        return []
    
    tracks = []
    for medium in data["release"].get("medium-list", []):
        for track in medium.get("track-list", []):
            tracks.append({
                "recording_mbid": track["recording"]["id"],
                "position": int(track["position"]),
                "title": track["recording"]["title"],
                "title_latin": transliterate(track["recording"]["title"])
            })
    
    return tracks

async def match_track(pool, track: dict, release_tracks: list, min_similarity: float = 0.55):
    """Match a local track to a MusicBrainz recording."""
    track_name = track["track_name"]
    track_number = track.get("track_number")
    
    if not track_number:
        return None
    
    # Filter by position
    position_matches = [r for r in release_tracks if r["position"] == track_number]
    if not position_matches:
        return None
    
    # Find best match
    best_score = 0
    best_candidate = None
    
    for candidate in position_matches:
        score_original = similarity_score(track_name, candidate["title"])
        score_translit = similarity_score(track_name, candidate["title_latin"])
        score = max(score_original, score_translit)
        
        if score > best_score:
            best_score = score
            best_candidate = candidate
    
    if best_candidate and best_score >= min_similarity:
        await pool.execute(
            "UPDATE tracks SET recording_mbid = $1 WHERE track_id = $2",
            best_candidate["recording_mbid"],
            track["track_id"]
        )
        print(f"🟢 Track '{track_name}' → '{best_candidate['title']}' (score={best_score:.2f})")
        return best_candidate["recording_mbid"]
    
    return None

# ============================================================
# MAIN PROCESSING
# ============================================================

async def process_artists(pool):
    """Process all artists missing MBIDs."""
    artists = await pool.fetch(
        "SELECT artist_id, artist_name, spotify_id FROM artists WHERE artist_mbid IS NULL AND on_mb IS NULL"
    )
    
    for artist in tqdm(artists, desc="Processing artists"):
        search_results = await safe_mb_call(
            musicbrainzngs.search_artists,
            artist["artist_name"],
            limit=25
        )
        
        if search_results and search_results.get("artist-list"):
            await match_and_assign_mbid(
                pool,
                "artists",
                artist["artist_id"],
                artist["artist_name"],
                search_results["artist-list"],
                artist.get("spotify_id")
            )
        
        await asyncio.sleep(0.5)

async def process_releases(pool):
    """Process all releases missing MBIDs."""
    releases = await pool.fetch("""
        SELECT r.release_id, r.release_name, r.spotify_id, a.artist_name
        FROM releases r
        JOIN artists a ON r.primary_artist_id = a.artist_id
        WHERE r.release_mbid IS NULL AND r.on_mb IS NULL
    """)
    
    for release in tqdm(releases, desc="Processing releases"):
        search_results = await safe_mb_call(
            musicbrainzngs.search_releases,
            release["release_name"],
            artist=release["artist_name"],
            limit=25
        )
        
        if search_results and search_results.get("release-list"):
            mbid = await match_and_assign_mbid(
                pool,
                "releases",
                release["release_id"],
                release["release_name"],
                search_results["release-list"],
                release.get("spotify_id")
            )
            
            # Process tracks for this release
            if mbid:
                await process_tracks_for_release(pool, release["release_id"], mbid)
        
        await asyncio.sleep(0.5)

async def process_tracks_for_release(pool, release_id: int, release_mbid: str):
    """Process all tracks for a specific release."""
    tracks = await pool.fetch(
        "SELECT track_id, track_name, track_number FROM tracks WHERE release_id = $1 AND recording_mbid IS NULL",
        release_id
    )
    
    if not tracks:
        return
    
    release_tracks = await fetch_release_tracks(release_mbid)
    
    for track in tracks:
        await match_track(pool, track, release_tracks)
        await asyncio.sleep(0.2)

async def process_orphan_tracks(pool):
    """Process tracks whose releases now have MBIDs but tracks don't."""
    tracks = await pool.fetch("""
        SELECT t.track_id, t.track_name, t.track_number, r.release_mbid
        FROM tracks t
        JOIN releases r ON t.release_id = r.release_id
        WHERE t.recording_mbid IS NULL
          AND r.release_mbid IS NOT NULL
          AND t.on_mb IS NULL
    """)
    
    for track in tqdm(tracks, desc="Processing orphan tracks"):
        release_tracks = await fetch_release_tracks(track["release_mbid"])
        await match_track(pool, track, release_tracks)
        await asyncio.sleep(0.5)

# ============================================================
# MAIN
# ============================================================

async def main():
    pool = await asyncpg.create_pool(**DB_CONFIG)
    
    print("🎵 Starting MusicBrainz matching process...")
    
    await process_artists(pool)
    await process_releases(pool)
    await process_orphan_tracks(pool)
    
    print("✅ MusicBrainz update completed!")
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())