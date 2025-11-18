import asyncio
import asyncpg
import musicbrainzngs
import functools
from tqdm import tqdm
from difflib import SequenceMatcher
from dotenv import load_dotenv
import os
import time
import re
import unicodedata

load_dotenv()

# -----------------------------
# database config
# -----------------------------
DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

# -----------------------------
# musicbrainz setup
# -----------------------------
mb_user_agent = os.getenv("MB_USER_AGENT")
if not mb_user_agent:
    raise ValueError("MB_USER_AGENT environment variable is missing!")
musicbrainzngs.set_useragent("ListenBrainzMatcher", "1.0", mb_user_agent)

# -----------------------------
# global cache
# -----------------------------
artist_cache = {}
release_cache = {}

RATE_LIMIT_DELAY = 1.1

# -----------------------------
# helpers
# -----------------------------
def normalize_name(name: str) -> str:
    if not name:
        return ""
    # Convert Unicode to a consistent normalized form (fix weird punctuation)
    name = unicodedata.normalize("NFKC", name)
    name = name.lower().strip()
    # Remove parentheses, brackets, version labels, remaster tags, etc.
    name = re.sub(r"\s*\(.*?\)", "", name)
    name = re.sub(r"\s*\[.*?\]", "", name)
    # Remove common suffixes that often differ between sources
    name = re.sub(r"[-–—]\s*(remaster(ed)?|version|bonus track|mono|stereo).*", "", name)
    # Remove punctuation except alphanumerics and spaces
    name = re.sub(r"[^\w\s\u3000\u3040-\u30FF\u4E00-\u9FFF]", "", name)
    # Collapse multiple spaces
    name = re.sub(r"\s+", " ", name)
    return name.strip()

def similar(a, b):
    return SequenceMatcher(None, normalize_name(a), normalize_name(b)).ratio()

async def safe_mb_call(func, *args, retries=5, delay=2, **kwargs):
    """Async wrapper for blocking MusicBrainz calls."""
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

def extract_spotify_id(url_relations):
    for rel in url_relations:
        target = rel.get("target", "")
        if "spotify.com/" in target:
            return target.split("/")[-1].split("?")[0]
    return None

# -----------------------------
# MBID assignment
# -----------------------------
async def assign_mb_id(
    pool,
    local_id,
    local_name,
    table,
    search_list,
    local_spotify_id=None,
    id_field=None,
    name_field=None,
    min_similarity=0.8
):
    """Assign MBID to artist or release, optionally filling missing Spotify ID."""
    # try Spotify ID match first
    for candidate in search_list:
        full_entity = await safe_mb_call(
            musicbrainzngs.get_artist_by_id if table == "artists" else musicbrainzngs.get_release_by_id,
            candidate["id"],
            includes=["url-rels"]
        )
        await asyncio.sleep(RATE_LIMIT_DELAY)
        if not full_entity or table[:-1] not in full_entity:
            continue

        mb_entity = full_entity[table[:-1]]
        mb_spotify_id = extract_spotify_id(mb_entity.get("url-relation-list", []))

        # Fill missing Spotify ID
        if not local_spotify_id and mb_spotify_id:
            await pool.execute(
                f"UPDATE {table} SET spotify_id=$1 WHERE {table[:-1]}_id=$2",
                mb_spotify_id, local_id
            )
            print(f"🟢 Filled missing Spotify ID for {table[:-1]} '{local_name}' → {mb_spotify_id}")

        # Assign MBID
        mbid = mb_entity["id"]
        await pool.execute(f"UPDATE {table} SET {id_field}=$1 WHERE {table[:-1]}_id=$2", mbid, local_id)
        print(f"🟢 {table[:-1].capitalize()} '{local_name}' → MBID {mbid} (Spotify ID match if available)")
        return mbid

    # fallback: best name similarity
    best_candidate = max(
        search_list,
        key=lambda c: similar(local_name, c.get(name_field, ""))
    )
    similarity_score = similar(local_name, best_candidate.get(name_field, ""))

    if similarity_score >= min_similarity:
        mbid = best_candidate["id"]
        # Attempt to fill missing Spotify ID
        full_entity = await safe_mb_call(
            musicbrainzngs.get_artist_by_id if table == "artists" else musicbrainzngs.get_release_by_id,
            mbid,
            includes=["url-rels"]
        )
        await asyncio.sleep(RATE_LIMIT_DELAY)
        if full_entity and table[:-1] in full_entity:
            mb_entity = full_entity[table[:-1]]
            mb_spotify_id = extract_spotify_id(mb_entity.get("url-relation-list", []))
            if not local_spotify_id and mb_spotify_id:
                await pool.execute(
                    f"UPDATE {table} SET spotify_id=$1 WHERE {table[:-1]}_id=$2",
                    mb_spotify_id, local_id
                )
                print(f"🟢 Filled missing Spotify ID for {table[:-1]} '{local_name}' → {mb_spotify_id}")

        # Assign MBID
        await pool.execute(f"UPDATE {table} SET {id_field}=$1 WHERE {table[:-1]}_id=$2", mbid, local_id)
        print(f"🟢 {table[:-1].capitalize()} '{local_name}' → MBID {mbid} (Name fallback, score={similarity_score:.2f})")
        return mbid

    print(f"🔴 No good MB match for {table[:-1]} '{local_name}'")
    return None

async def assign_track_mbid(pool, track, recordings, min_similarity=0.85):
    """Assign MBID to a track, allowing duplicates."""
    if not recordings:
        print(f"🔴 No recordings found for track '{track['track_name']}'")
        return None

    best = max(recordings, key=lambda r: similar(track["track_name"], r["title"]))
    score = similar(track["track_name"], best["title"])

    if score < min_similarity:
        print(f"🔴 No good MB match for track '{track['track_name']}' (best score={score:.2f})")
        return None

    recording_mbid = best["id"]
    track_id = track["track_id"]

    # Assign MBID regardless of duplicates
    await pool.execute(
        "UPDATE tracks SET recording_mbid=$1 WHERE track_id=$2",
        recording_mbid, track_id
    )

    # Fill missing Spotify ID if available
    mb_spotify_id = extract_spotify_id(best.get("relations", []))
    if mb_spotify_id and not track.get("spotify_id"):
        await pool.execute(
            "UPDATE tracks SET spotify_id=$1 WHERE track_id=$2",
            mb_spotify_id, track_id
        )
        print(f"🟢 Filled missing Spotify ID for track '{track['track_name']}' → {mb_spotify_id}")

    print(f"🟢 Track '{track['track_name']}' → MBID {recording_mbid} (score={score:.2f})")
    return recording_mbid

# -----------------------------
# fetch release recordings
# -----------------------------
async def fetch_release_recordings(release_mbid):
    release_info = await safe_mb_call(
        musicbrainzngs.get_release_by_id,
        release_mbid,
        includes=["recordings"]
    )
    if not release_info or not release_info.get("release"):
        return []

    recordings = []
    for medium in release_info["release"].get("medium-list", []):
        for track in medium.get("track-list", []):
            recordings.append(track["recording"])

    release_cache[release_mbid] = recordings
    return recordings

# -----------------------------
# main processing
# -----------------------------
async def process_artist(pool, artist):
    search_data = await safe_mb_call(musicbrainzngs.search_artists, artist["artist_name"], limit=25)
    if not search_data or not search_data.get("artist-list"):
        print(f"🔴 Artist '{artist['artist_name']}' not found on MB")
        return None

    return await assign_mb_id(
        pool=pool,
        local_id=artist["artist_id"],
        local_name=artist["artist_name"],
        table="artists",
        search_list=search_data["artist-list"],
        local_spotify_id=artist.get("spotify_id"),
        id_field="artist_mbid",
        name_field="name",
        min_similarity=0.8
    )

async def process_release(pool, release, artist_name):
    search_data = await safe_mb_call(
        musicbrainzngs.search_releases,
        release["release_name"],
        artist=artist_name,
        limit=25
    )
    if not search_data or not search_data.get("release-list"):
        print(f"🔴 Release '{release['release_name']}' not found on MB")
        return None

    return await assign_mb_id(
        pool=pool,
        local_id=release["release_id"],
        local_name=release["release_name"],
        table="releases",
        search_list=search_data["release-list"],
        local_spotify_id=release.get("spotify_id"),
        id_field="release_mbid",
        name_field="title",
        min_similarity=0.8
    )

async def process_track(pool, track, release_mbid=None):
    if track.get("recording_mbid"):
        return

    recordings = []
    if release_mbid:
        recordings = await fetch_release_recordings(release_mbid)

    if not recordings:
        # fallback: search track by name + artist
        search_data = await safe_mb_call(
            musicbrainzngs.search_recordings,
            track["track_name"],
            artist=track["artist_name"],
            limit=10
        )
        if search_data and search_data.get("recording-list"):
            recordings = search_data["recording-list"]

    await assign_track_mbid(pool, track, recordings)

# -----------------------------
# run all
# -----------------------------
async def main():
    pool = await asyncpg.create_pool(**DB_CONFIG)

    # -----------------------------
    # 1️⃣ Process artists missing MBID
    # -----------------------------
    artists = await pool.fetch(
        """
        SELECT artist_id, artist_name, spotify_id, artist_mbid
        FROM artists
        WHERE artist_mbid IS NULL;
        """
    )

    for artist in tqdm(artists, desc="Artists missing MBID"):
        await process_artist(pool, artist)
        await asyncio.sleep(0.5)  # slight delay to pace MB requests

    # -----------------------------
    # 2️⃣ Process releases missing MBID (regardless of artist MBID)
    # -----------------------------
    releases = await pool.fetch(
        """
        SELECT r.release_id, r.release_name, r.release_mbid, r.spotify_id,
               a.artist_name, a.artist_id
        FROM releases r
        JOIN artists a ON r.primary_artist_id = a.artist_id
        WHERE r.release_mbid IS NULL;
        """
    )

    for release in tqdm(releases, desc="Releases missing MBID"):
        await process_release(pool, release, release["artist_name"])
        await asyncio.sleep(0.5)

    # -----------------------------
    # 3️⃣ Process tracks missing MBID (regardless of release MBID)
    # -----------------------------
    tracks = await pool.fetch(
        """
        SELECT t.track_id, t.track_name, t.recording_mbid, t.spotify_id,
               r.release_name, r.release_id, a.artist_name, a.artist_id
        FROM tracks t
        JOIN releases r ON t.release_id = r.release_id
        JOIN artists a ON r.primary_artist_id = a.artist_id
        WHERE t.recording_mbid IS NULL;
        """
    )
    
    for track in tqdm(tracks, desc="Tracks missing MBID"):
        release_mbid = track.get("release_mbid")
        
        if release_mbid:
            # Normal flow: fetch recordings from release MBID
            await process_track(pool, track, release_mbid=release_mbid)
        else:
            # Fallback: search MusicBrainz directly by track + artist
            search_data = await safe_mb_call(
                musicbrainzngs.search_recordings,
                track["track_name"],
                artist=track["artist_name"],
                limit=5
            )
            recordings = search_data.get("recording-list", []) if search_data else []
            if recordings:
                await assign_track_mbid(pool, track, recordings, min_similarity=0.7)

        await asyncio.sleep(0.5)

    await pool.close()
    print("😛 MusicBrainz update completed!")

if __name__ == "__main__":
    asyncio.run(main())
