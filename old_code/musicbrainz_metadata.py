import asyncio
import aiohttp
import asyncpg
from datetime import datetime, timedelta, timezone
import os
import logging
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

MB_API = "https://musicbrainz.org/ws/2"
AB_API = "https://acousticbrainz.org/api/v1"
WD_API = "https://www.wikidata.org/w/api.php"
HEADERS = {"User-Agent": os.getenv("MB_USER_AGENT")}
REQUEST_DELAY = 0.3

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger()
logger.handlers[0].setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%dT%H:%M:%SZ"))

# -----------------------------
# HELPERS
# -----------------------------
def parse_date(date_val):
    """
    Convert a MusicBrainz date string to datetime.date.
    Handles 'YYYY', 'YYYY-MM', 'YYYY-MM-DD'.
    Returns None for None or non-string values.
    """
    if not isinstance(date_val, str):
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(date_val, fmt).date()
        except ValueError:
            continue
    return None

async def refresh_metadata(pool, table, row_id_col, row_id, threshold_days=30):
    """
    Returns True if the row should be refreshed (retrieved_at is older than threshold_days or null).
    Uses timezone-aware UTC datetimes to avoid deprecation warnings.
    """
    query = f"""
        SELECT retrieved_at
        FROM {table}
        WHERE {row_id_col} = $1
    """
    retrieved_at = await pool.fetchval(query, row_id)

    if not retrieved_at:
        return True  # no timestamp, need to fetch

    # Convert to timezone-aware UTC if naive
    if retrieved_at.tzinfo is None:
        retrieved_at = retrieved_at.replace(tzinfo=timezone.utc)

    if datetime.now(timezone.utc) - retrieved_at > timedelta(days=threshold_days):
        return True  # older than threshold, refresh

    return False  # recent enough, skip

def build_artist_map(credits):
    artist_map = {}

    for c in credits:
        artist = c.get("artist")
        if not artist:
            continue

        mbid = artist.get("id")
        if not mbid:
            continue

        name = (
            c.get("name")
            or artist.get("name")
            or artist.get("sort-name")
        )

        if not name or not isinstance(name, str):
            name = "[MB:missing-credit]"

        artist_map[mbid] = name.strip() or "[MB:missing-credit]"

    return artist_map

async def safe_request(session, url, params=None, retries=5, delay=1.2):
    if not url:
        return None

    for attempt in range(retries):
        try:
            async with session.get(url, params=params, headers=HEADERS) as resp:
                if resp.status == 503:
                    await asyncio.sleep(2 ** attempt)
                    continue
                if resp.status == 400:
                    return None
                if resp.status != 200:
                    return None

                try:
                    return await resp.json()
                except Exception:
                    return None
        except Exception:
            await asyncio.sleep(2 ** attempt)
        finally:
            await asyncio.sleep(delay)

    return None

# -----------------------------
# MUSICBRAINZ FETCHERS
# -----------------------------
async def fetch_artist_mbid(session, mbid):
    if not mbid:
        return None
    params = {"fmt": "json", "inc": "aliases+genres"}
    return await safe_request(session, f"{MB_API}/artist/{mbid}", params=params)

async def fetch_release_group(session, rg_mbid):
    """
    Fetch release-groups for an artist, including releases.
    Each release-group contains its release, so we can get the primary-type.
    """
    if not rg_mbid:
        return None
    params = {"fmt": "json", "inc": "genres"}
    return await safe_request(session, f"{MB_API}/release-group/{rg_mbid}", params=params)

async def fetch_release(session, mbid):
    if not mbid:
        return []

    params = {
        "fmt": "json",
        "inc": "artist-credits recordings genres"    
    }
    return await safe_request(session, f"{MB_API}/release/{mbid}", params=params)

# -----------------------------
# WIKIDATA GENRE FETCHERS
# -----------------------------

# Cache for genre MBID -> Wikidata ID mappings to avoid redundant lookups
GENRE_CACHE = {}

async def fetch_wikidata_entity(session, wikidata_id):
    """
    Fetch a Wikidata entity by its QID (e.g., 'Q11399').
    Returns the entity data or None if not found.
    """
    if not wikidata_id or not wikidata_id.startswith('Q'):
        return None
    
    params = {
        "action": "wbgetentities",
        "ids": wikidata_id,
        "format": "json",
        "languages": "en"
    }
    
    data = await safe_request(session, WD_API, params=params, delay=0.1)
    if not data or "entities" not in data:
        return None
    
    return data["entities"].get(wikidata_id)

async def get_wikidata_from_mb_genre(session, genre_mbid, fallback_search=True):
    """
    Fetch a MusicBrainz genre and extract its Wikidata QID.

    Strategy:
    1. Look up Wikidata item with P8052 = genre_mbid (exact match).
    2. If not found, use MusicBrainz genre URL relations.
    3. If still not found and fallback_search=True, search Wikidata by genre name.

    Args:
        session: aiohttp ClientSession
        genre_mbid: MusicBrainz genre UUID
        fallback_search: whether to search by name if no link found

    Returns:
        Wikidata QID (e.g., "Q1055871") or None
    """
    if not genre_mbid:
        return None

    # Check cache
    if genre_mbid in GENRE_CACHE:
        return GENRE_CACHE[genre_mbid]

    # 1️⃣ Try SPARQL query using P8052
    sparql_query = f"""
    SELECT ?item WHERE {{
      ?item wdt:P8052 "{genre_mbid}" .
    }}
    LIMIT 1
    """
    sparql_url = "https://query.wikidata.org/sparql"
    try:
        async with session.get(sparql_url, params={"query": sparql_query, "format": "json"}, headers=HEADERS) as resp:
            if resp.status == 200:
                data = await resp.json()
                results = data.get("results", {}).get("bindings", [])
                if results:
                    qid = results[0]["item"]["value"].rstrip("/").split("/")[-1]
                    GENRE_CACHE[genre_mbid] = qid
                    logger.info(f"🟢 P8052 mapping: MB genre {genre_mbid} → Wikidata {qid}")
                    return qid
    except Exception:
        pass  # quietly fallback if SPARQL fails

    # 2️⃣ Try MusicBrainz URL relation
    params = {"fmt": "json", "inc": "url-rels"}
    mb_data = await safe_request(session, f"{MB_API}/genre/{genre_mbid}", params=params, delay=REQUEST_DELAY)
    if mb_data:
        relations = mb_data.get("relations", [])
        for rel in relations:
            url_obj = rel.get("url", {})
            if not url_obj:
                continue
            url = url_obj.get("resource", "")
            rel_type = (rel.get("type") or "").lower()
            if "wikidata" in rel_type and "wikidata.org/wiki/" in url:
                qid = url.rstrip("/").split("/")[-1]
                GENRE_CACHE[genre_mbid] = qid
                logger.info(f"🔗 MB URL mapping: genre {mb_data.get('name', 'unknown')} ({genre_mbid}) → Wikidata {qid}")
                return qid

    # 3️⃣ Fallback: search by name if enabled
    if fallback_search and mb_data:
        genre_name = mb_data.get("name", "")
        qid = await search_wikidata_for_genre(session, genre_name)
        if qid:
            GENRE_CACHE[genre_mbid] = qid
            logger.info(f"🔍 Fallback search: MB genre {genre_name} ({genre_mbid}) → Wikidata {qid}")
            return qid

    # Nothing found
    GENRE_CACHE[genre_mbid] = None
    logger.warning(f"⚠️ No Wikidata link found for genre {mb_data.get('name', genre_mbid)} ({genre_mbid})")
    return None

MUSIC_GENRE_ROOTS = {"Q188451", "Q373342"}  # music genre, popular music

# 1. Absolute limits. If we hit these, the "tree" stops immediately.
STOP_QIDS = {
    "Q483394",    # genre
    "Q1792379",   # art genre
    "Q11696608",  # elements of music
    "Q115211517", # musical concept
    "Q35120",     # entity
    "Q11461",      # sound (Physics)
    "Q639197",     # instrumental music (Too broad)
    "Q115484611", # music
    "Q862597",     # musical form
    "Q28820001",   # musical scene
    "Q18378428",   # music community
    "Q75054287",   # music by country or region
    "Q3328755",    # regional music
    "Q62906332",   # entertainment music
}

# 2. Relevancy check. Helps distinguish "Music Genre" from "Literary Genre".
MUSIC_KEYWORDS = {"music", "musical", "genre", "style", "sound"}

async def get_genre_hierarchy(session, wikidata_id, max_depth=2, _current_depth=0):
    """
    Fetches genre parents with strict MusicBrainz validation.
    Max depth 2 is usually sufficient for: Sub-genre -> Genre -> Root.
    """
    if not wikidata_id or _current_depth >= max_depth or wikidata_id in STOP_QIDS:
        return []

    entity = await fetch_wikidata_entity(session, wikidata_id)
    if not entity:
        return []

    claims = entity.get("claims", {})
    labels = entity.get("labels", {})
    name = labels.get("en", {}).get("value", wikidata_id)
    desc = entity.get("descriptions", {}).get("en", {}).get("value", "").lower()

    # 🛡️ THE MUSICBRAINZ "SILVER BULLET" VALIDATION
    # We skip parents that don't look like actual music genres.
    if _current_depth > 0:
        has_mb_link = "P8052" in claims
        # 'musical genre' description check is a safety net for new/unlinked items
        is_genre_desc = any(kw in desc for kw in ["music genre", "musical genre", "genre of music"])
        
        if not (has_mb_link or is_genre_desc):
            logger.info(f"⏩ Skipping non-genre/non-MB parent: {name} ({wikidata_id})")
            return []

    hierarchy = [(wikidata_id, name, _current_depth)]
    
    potential_parents = set()
    for prop in ["P279", "P31"]:
        for claim in claims.get(prop, []):
            try:
                p_id = claim["mainsnak"]["datavalue"]["value"]["id"]
                if p_id not in STOP_QIDS:
                    potential_parents.add(p_id)
            except (KeyError, TypeError):
                continue

    for p_id in potential_parents:
        parent_results = await get_genre_hierarchy(session, p_id, max_depth, _current_depth + 1)
        # Avoid duplicates in the same hierarchy branch
        for res in parent_results:
            if res[0] not in [h[0] for h in hierarchy]:
                hierarchy.append(res)
        
        await asyncio.sleep(0.1) # Respect Wikidata's rate limits

    return hierarchy

async def search_wikidata_for_genre(session, genre_name):
    """
    Search Wikidata for a music genre by name.
    Returns the Wikidata QID if found, None otherwise.
    """
    if not genre_name:
        return None
    
    # Search for the genre
    params = {
        "action": "wbsearchentities",
        "search": genre_name,
        "language": "en",
        "format": "json",
        "type": "item",
        "limit": 10
    }
    
    data = await safe_request(session, WD_API, params=params, delay=0.1)
    if not data or "search" not in data:
        return None
    
    # Look through results for music genre matches
    for result in data["search"]:
        qid = result.get("id")
        description = result.get("description", "").lower()
        label = result.get("label", "").lower()
        
        # Check if it's actually a music genre
        # Look for descriptions containing "music genre" or exact label match
        if "music genre" in description or "musical genre" in description:
            if label == genre_name.lower():
                return qid
            # Also accept close matches for common variations
            if label.replace("-", " ") == genre_name.lower().replace("-", " "):
                return qid
    
    # If no exact match with music genre description, try first result if label matches
    if data["search"]:
        first = data["search"][0]
        if first.get("label", "").lower() == genre_name.lower():
            return first.get("id")
    
    return None

# -----------------------------
# ACOUSTICBRAINZ
# -----------------------------
async def fetch_acoustic_features(session, recording_mbid):
    data = await safe_request(session, f"{AB_API}/{recording_mbid}/low-level")
    return data

# -----------------------------
# UPSERT HELPERS
# -----------------------------
async def update_artist(pool, artist_id, artist_data):
    """
    Safely updates an existing artist row in the database.
    - artist_id: primary key of the artist to update
    - artist_data: JSON dict from MusicBrainz
    """
    # Parse dates safely
    start_date = parse_date(artist_data.get("life-span", {}).get("begin"))
    end_date = parse_date(artist_data.get("life-span", {}).get("end"))

    # Ensure artist_type is never NULL
    # If missing in MusicBrainz, keep existing value in DB
    artist_type = artist_data.get("type")
    if artist_type is None:
        artist_type = await pool.fetchval(
            "SELECT artist_type FROM artists WHERE artist_id=$1", artist_id
        )

    # Update the artist row
    result = await pool.execute("""
        UPDATE artists
        SET artist_name = $1,
            sort_name = $2,
            artist_type = $3,
            start_date = COALESCE(start_date, $4),
            end_date = COALESCE(end_date, $5),
            country = COALESCE(country, $6)
        WHERE artist_id = $7
    """,
    artist_data['name'],
    artist_data['sort-name'],
    artist_type,
    start_date,
    end_date,
    artist_data.get("country"),
    artist_id
    )
    if result.endswith("0"):
        print(f"⚠️ No rows updated (artist_id={artist_id})")

async def update_artist_aliases(pool, artist_id, artist_data):
    aliases = artist_data.get("aliases", [])
    async with pool.acquire() as conn:
        for alias in aliases:
            name = alias.get("name")
            if not name:
                continue
            locale = alias.get("locale")
            primary = alias.get("primary", False)
            source = "MusicBrainz"
            await conn.execute("""
                    INSERT INTO artist_aliases (artist_id, alias_name, source,
                        locale, primary_alias, retrieved_at)
                    VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                    ON CONFLICT (artist_id, alias_name)
                    DO UPDATE SET primary_alias = EXCLUDED.primary_alias,
                                    retrieved_at = CURRENT_TIMESTAMP;
                """, artist_id, name, source, locale, primary)

async def get_or_create_artist_id(pool, mbid, name):
    """
    Returns the internal artist_id for a given MBID.
    If the MBID doesn't eist in the database yet, insert a minimal row.
    """
    async with pool.acquire() as conn:
        artist_id = await conn.fetchval(
            "SELECT artist_id FROM artists WHERE artist_mbid = $1",
            mbid
        )
        if artist_id:
            return artist_id
        
        # insert minimal row
        await conn.execute("""
            INSERT INTO artists (artist_mbid, artist_name, sort_name, artist_type, on_mb)
            VALUES ($1, $2, $2, 'Group', TRUE)
            ON CONFLICT (artist_mbid) DO NOTHING                   
        """, mbid, name)
        logger.info(f"🆕 Created new artist: {name} ({mbid})")
        
        # fetch again to get the new artist_id
        artist_id = await conn.fetchval(
            "SELECT artist_id FROM artists WHERE artist_mbid = $1",
            mbid
        )
        return artist_id

async def update_release(pool, db_release_id, release_group, release):
    release_date = parse_date(release_group.get('first-release-date'))
    release_type = release_group.get("primary-type")
    
    text_rep = release.get("text-representation") or {}
    language = text_rep.get("language")

    result = await pool.execute("""
        UPDATE releases
        SET release_type = COALESCE($1, release_type),
            release_date = COALESCE($2, release_date),
            language = COALESCE($3, language)
        WHERE release_id = $4
    """,
    release_type,
    release_date,
    language,
    db_release_id
    )

    if result.endswith("0"):
        print(f"⚠️ Release not updated (release_id={db_release_id})")

async def update_track(pool, db_track_id, track_data):
    if "recording" not in track_data:
        return

    recording = track_data['recording']

    result = await pool.execute("""
        UPDATE tracks
        SET track_name = COALESCE(track_name, $1),
            duration_ms = COALESCE(duration_ms, $2),
            track_number = COALESCE(track_number, $3)
        WHERE track_id = $4
    """,
    track_data.get('title'),
    recording.get('length'),
    track_data.get('position'),
    db_track_id
    )
    
    if result.endswith("0"):
        print(f"⚠️ Track not updated (track_id={db_track_id})")
        
async def upsert_acoustic_features(pool, track_id, features):
    """
    Inserts low-level features from AcousticBrainz.
    Skips if features are missing or track_id is None.
    """
    
    if not features or not track_id:
        return
    
    exists = await pool.fetchval(
        "SELECT 1 FROM track_audio_features WHERE track_Id = $1",
        track_id
    )
    if exists:
        return
    
    lowlevel = features.get("lowlevel", {})
    tonal = features.get("tonal", {})
    rhythm = features.get("rhythm", {})
    

    await pool.execute("""
        INSERT INTO track_audio_features
        (track_id, loudness, dynamic_complexity, bpm, danceability,
        chords_complexity, key, scale, tuning_frequency)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (track_id) DO NOTHING
    """,
        track_id,
        lowlevel.get("average_loudness"),
        lowlevel.get("dynamic_complexity"),
        rhythm.get("bpm"),
        rhythm.get("danceability"),
        tonal.get("chords_changes_rate"),
        tonal.get("key_key"),
        tonal.get("key_scale"),
        tonal.get("tuning_frequency")
    )

async def upsert_release_artists_batch(pool, release_id, release_data):
    credits = release_data.get("artist-credit", [])
    artist_map = build_artist_map(credits)
    if not artist_map:
        return
    mbids = list(artist_map.keys())

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT artist_id, artist_mbid FROM artists WHERE artist_mbid = ANY($1)", mbids
        )
        existing = {row['artist_mbid']: row['artist_id'] for row in rows}

        missing = [mbid for mbid in mbids if mbid not in existing]
        if missing:
            await conn.executemany("""
                INSERT INTO artists (artist_mbid, artist_name, sort_name, artist_type, on_mb)
                VALUES ($1, $2, $2, 'Group', TRUE)
                ON CONFLICT (artist_mbid) DO NOTHING
            """, [(mbid, artist_map[mbid]) for mbid in missing])
            for mbid in missing:
                logger.info(f"🆕 Created new artist: {artist_map[mbid]} ({mbid})")

            rows = await conn.fetch(
                "SELECT artist_id, artist_mbid FROM artists WHERE artist_mbid = ANY($1)", missing
            )
            for row in rows:
                existing[row['artist_mbid']] = row['artist_id']

        await conn.executemany("""
            INSERT INTO release_artists (release_id, artist_id, retrieved_at)
            VALUES ($1, $2, CURRENT_TIMESTAMP)
            ON CONFLICT (release_id, artist_id)
            DO UPDATE SET retrieved_at = CURRENT_TIMESTAMP;
        """, [(release_id, artist_id) for artist_id in existing.values()])
        logger.info(f"✅ release_artists updated for release_id={release_id}, artists={list(existing.values())}")

async def upsert_track_artists_batch(pool, track_id, track_data):
    credits = track_data.get("artist-credit", [])
    if not credits:
        return

    artist_map = build_artist_map(credits)
    if not artist_map:
        return
    mbids = list(artist_map.keys())

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT artist_id, artist_mbid FROM artists WHERE artist_mbid = ANY($1)", mbids
        )
        existing = {row['artist_mbid']: row['artist_id'] for row in rows}

        missing = [mbid for mbid in mbids if mbid not in existing]
        if missing:
            await conn.executemany("""
                INSERT INTO artists (artist_mbid, artist_name, sort_name, artist_type, on_mb)
                VALUES ($1, $2, $2, 'Group', TRUE)
                ON CONFLICT (artist_mbid) DO NOTHING
            """, [(mbid, artist_map[mbid]) for mbid in missing])
            for mbid in missing:
                logger.info(f"🆕 Created new artist: {artist_map[mbid]} ({mbid})")

            rows = await conn.fetch(
                "SELECT artist_id, artist_mbid FROM artists WHERE artist_mbid = ANY($1)", missing
            )
            for row in rows:
                existing[row['artist_mbid']] = row['artist_id']

        await conn.executemany("""
            INSERT INTO track_artists (track_id, artist_id, retrieved_at)
            VALUES ($1, $2, CURRENT_TIMESTAMP)
            ON CONFLICT (track_id, artist_id)
            DO UPDATE SET retrieved_at = CURRENT_TIMESTAMP;
        """, [(track_id, artist_id) for artist_id in existing.values()])
        logger.info(f"✅ track_artists updated for track_id={track_id}, artists={list(existing.values())}")

# -----------------------------
# GENRE HELPERS
# -----------------------------
async def get_or_create_genre(pool, wikidata_id, genre_name):
    """
    Get or create a genre in the database by Wikidata ID.
    Returns the genre_id.
    """
    if not wikidata_id:
        return None

    wikidata_id = str(wikidata_id)  # Ensure string for DB TEXT column

    async with pool.acquire() as conn:
        # Check if genre already exists
        genre_id = await conn.fetchval(
            "SELECT genre_id FROM genres WHERE wikidata_id = $1",
            wikidata_id
        )
        if genre_id:
            return genre_id

        # Insert new genre (or update name if conflict on wikidata_id)
        genre_id = await conn.fetchval("""
            INSERT INTO genres (wikidata_id, genre_name, retrieved_at)
            VALUES ($1, $2, CURRENT_TIMESTAMP)
            ON CONFLICT (wikidata_id) DO UPDATE 
            SET genre_name = EXCLUDED.genre_name,
                retrieved_at = CURRENT_TIMESTAMP
            RETURNING genre_id
        """, wikidata_id, genre_name)

        logger.info(f"🎵 Created genre: {genre_name} ({wikidata_id})")
        return genre_id

async def upsert_genre_hierarchy(pool, child_wikidata_id, parent_wikidata_id, depth):
    """
    Insert or update a parent-child relationship in the genre hierarchy.
    Ensures all Wikidata IDs are strings to match DB TEXT column.
    """
    if not child_wikidata_id or not parent_wikidata_id:
        return

    child_wikidata_id = str(child_wikidata_id)
    parent_wikidata_id = str(parent_wikidata_id)

    async with pool.acquire() as conn:
        # Get genre IDs
        child_id = await conn.fetchval(
            "SELECT genre_id FROM genres WHERE wikidata_id = $1",
            child_wikidata_id
        )
        parent_id = await conn.fetchval(
            "SELECT genre_id FROM genres WHERE wikidata_id = $1",
            parent_wikidata_id
        )

        if not child_id or not parent_id:
            return

        # Insert hierarchy relationship
        await conn.execute("""
            INSERT INTO genre_hierarchy (child_genre_id, parent_genre_id, depth)
            VALUES ($1, $2, $3)
            ON CONFLICT (child_genre_id, parent_genre_id) 
            DO UPDATE SET depth = LEAST(genre_hierarchy.depth, EXCLUDED.depth)
        """, child_id, parent_id, depth)

async def process_genres_from_mb(session, pool, mb_genres, entity_type, entity_id):
    if not mb_genres:
        return

    for mb_genre in mb_genres:
        genre_mbid = mb_genre.get("id")
        genre_name = mb_genre.get("name", "Unknown")

        if not genre_mbid:
            logger.warning(f"⚠️ Genre missing MBID: {genre_name}")
            continue

        # Get Wikidata ID from MusicBrainz genre (P8052 / MB URL / fallback search)
        wikidata_id = await get_wikidata_from_mb_genre(session, genre_mbid)
        if not wikidata_id:
            logger.warning(f"⚠️ No Wikidata link for genre: {genre_name} ({genre_mbid})")
            continue

        wikidata_id = str(wikidata_id)  # Ensure string

        # Get the full genre hierarchy
        hierarchy = await get_genre_hierarchy(session, wikidata_id)

        # Create all genres in the hierarchy
        created_ids = {}
        for wd_id, wd_name, depth in hierarchy:
            wd_id = str(wd_id)
            # Skip storing generic root genres in DB if you want
            if wd_id in MUSIC_GENRE_ROOTS:
                continue
            genre_id = await get_or_create_genre(pool, wd_id, wd_name)
            created_ids[wd_id] = genre_id

        # Insert hierarchy relationships
        base_genre_id = created_ids.get(wikidata_id)
        for wd_id, _, depth in hierarchy:
            wd_id = str(wd_id)
            if wd_id == wikidata_id or wd_id not in created_ids:
                continue
            parent_genre_id = created_ids.get(wd_id)
            if parent_genre_id:
                await upsert_genre_hierarchy(pool, wikidata_id, wd_id, depth)

        # Link base genre to the entity
        if base_genre_id:
            async with pool.acquire() as conn:
                if entity_type == 'artist':
                    await conn.execute("""
                        INSERT INTO artist_genres (artist_id, genre_id, retrieved_at)
                        VALUES ($1, $2, CURRENT_TIMESTAMP)
                        ON CONFLICT (artist_id, genre_id) 
                        DO UPDATE SET retrieved_at = CURRENT_TIMESTAMP
                    """, entity_id, base_genre_id)
                elif entity_type == 'release':
                    await conn.execute("""
                        INSERT INTO release_genres (release_id, genre_id, retrieved_at)
                        VALUES ($1, $2, CURRENT_TIMESTAMP)
                        ON CONFLICT (release_id, genre_id) 
                        DO UPDATE SET retrieved_at = CURRENT_TIMESTAMP
                    """, entity_id, base_genre_id)

        logger.info(f"✅ Processed genre hierarchy for {genre_name}: {len(hierarchy)} levels")
        await asyncio.sleep(0.15)

# -----------------------------
# MAIN
# -----------------------------
async def main():
    pool = await asyncpg.create_pool(**DB_CONFIG)
    async with aiohttp.ClientSession() as session:
        # Artists
        artist_rows = await pool.fetch("SELECT artist_id, artist_mbid FROM artists WHERE on_mb IS TRUE;")
        for row in artist_rows:
            artist_id = row['artist_id']
            mbid = row['artist_mbid']
            if not await refresh_metadata(pool, "artists", "artist_id", artist_id, threshold_days=30):
                continue
            artist_data = await safe_request(session, f"{MB_API}/artist/{mbid}", params={"fmt": "json", "inc": "aliases+genres"})
            if not artist_data:
                logger.warning(f"⚠️ Failed to fetch artist {mbid}")
                continue
            await update_artist(pool, artist_id, artist_data)
            await update_artist_aliases(pool, artist_id, artist_data)
            
            # Process genres
            mb_genres = artist_data.get("genres", [])
            await process_genres_from_mb(session, pool, mb_genres, 'artist', artist_id)
            
            await pool.execute("UPDATE artists SET retrieved_at = CURRENT_TIMESTAMP WHERE artist_id=$1", artist_id)
            await asyncio.sleep(REQUEST_DELAY)

        # Releases
        releases = await pool.fetch("SELECT release_id, release_group_mbid, release_mbid FROM releases WHERE on_mb IS TRUE;")
        for row in releases:
            release_id = row['release_id']
            release_group_mbid = row['release_group_mbid']
            release_mbid = row['release_mbid']
            if not await refresh_metadata(pool, "releases", "release_id", release_id, threshold_days=30):
                continue
            rg_data = await safe_request(session, f"{MB_API}/release-group/{release_group_mbid}", params={"fmt": "json", "inc": "genres"})
            release_data = await safe_request(session, f"{MB_API}/release/{release_mbid}", params={"fmt": "json", "inc": "artist-credits recordings genres"})
            if not rg_data or not release_data:
                logger.warning(f"⚠️ Failed to fetch release {release_mbid}")
                continue
            await update_release(pool, release_id, rg_data, release_data)
            await upsert_release_artists_batch(pool, release_id, release_data)
            
            # Process genres from release-group
            mb_genres = rg_data.get("genres", [])
            await process_genres_from_mb(session, pool, mb_genres, 'release', release_id)
            
            await pool.execute("UPDATE releases SET retrieved_at=CURRENT_TIMESTAMP WHERE release_id=$1", release_id)
            await asyncio.sleep(REQUEST_DELAY)

        # Tracks
        tracks_with_mb = await pool.fetch("SELECT track_id, recording_mbid FROM tracks WHERE on_mb IS TRUE;")
        for row in tracks_with_mb:
            track_id = row['track_id']
            recording_mbid = row['recording_mbid']
            if not await refresh_metadata(pool, "tracks", "track_id", track_id, threshold_days=30):
                continue
            recording_data = await safe_request(session, f"{MB_API}/recording/{recording_mbid}", params={"fmt": "json", "inc": "artist-credits"})
            if not recording_data:
                logger.warning(f"⚠️ Failed to fetch recording {recording_mbid}")
                continue
            await update_track(pool, track_id, recording_data)
            await upsert_track_artists_batch(pool, track_id, recording_data)
            
            features = await fetch_acoustic_features(session, recording_mbid)
            await upsert_acoustic_features(pool, track_id, features)
            await pool.execute("UPDATE tracks SET retrieved_at=CURRENT_TIMESTAMP WHERE track_id=$1", track_id)
            await asyncio.sleep(REQUEST_DELAY)

    await pool.close()

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    asyncio.run(main())