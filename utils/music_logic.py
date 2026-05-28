import re
from typing import List, Dict, Optional
import asyncpg

# Pre-compile the regex for performance (important for 112k rows!)
ARTIST_SPLIT_REGEX = re.compile(r'\s*(?:,|&|\bfeat\.?\b|\bft\.?\b|\bwith\b|\band\b)\s*', re.IGNORECASE)

def build_artist_map_from_mbids(mbids: List[str], raw_artist_name: str) -> Dict[str, str]:
    """Build a {mbid: name} map from a list of MBIDs using best-effort name splitting."""
    if not mbids:
        return {}

    parts = [p.strip() for p in ARTIST_SPLIT_REGEX.split(raw_artist_name) if p.strip()]
    
    artist_map = {}
    for i, mbid in enumerate(mbids):
        # Fallback to a stub name if the split doesn't match the MBID count
        name = parts[i] if i < len(parts) else f"Artist {mbid[:8]}"
        artist_map[mbid] = name

    return artist_map

async def link_contributing_artists(conn: asyncpg.Connection, item_id: int, artist_mbid_map: Dict[str, str], mode: str = 'track'):
    """
    Links multiple artists to a track or release. 
    Creates stub artist records if the MBID isn't found.
    """
    if not artist_mbid_map:
        return

    table = "track_artists" if mode == 'track' else "release_artists"
    id_col = "track_id" if mode == 'track' else "release_id"
    mbids = list(artist_mbid_map.keys())

    # 1. Find existing artists by MBID
    rows = await conn.fetch("SELECT artist_id, artist_mbid FROM artists WHERE artist_mbid = ANY($1)", mbids)
    existing = {row['artist_mbid']: row['artist_id'] for row in rows}

    # 2. Create stubs for missing MBIDs
    # 2. Create stubs for missing MBIDs
    missing = [mbid for mbid in mbids if mbid not in existing]
    if missing:
        # Change: Use an UPSERT that updates the name if we only had the MBID stub
        await conn.executemany(
            """INSERT INTO artists (artist_mbid, artist_name, sort_name, artist_type, on_mb)
               VALUES ($1, $2, $2, 'Group', TRUE)
               ON CONFLICT (artist_mbid) DO UPDATE 
               SET artist_name = EXCLUDED.artist_name 
               WHERE artists.artist_name LIKE 'Artist %'""", 
            [(mbid, artist_mbid_map[mbid]) for mbid in missing]
        )
        
        # Change: Re-fetch ALL MBIDs in the set to ensure 'existing' is complete
        rows = await conn.fetch("SELECT artist_id, artist_mbid FROM artists WHERE artist_mbid = ANY($1)", mbids)
        existing = {row['artist_mbid']: row['artist_id'] for row in rows}

    # 3. Link everything to the join table
    if existing:
        await conn.executemany(
            f"""INSERT INTO {table} ({id_col}, artist_id, retrieved_at)
               VALUES ($1, $2, CURRENT_TIMESTAMP)
               ON CONFLICT ({id_col}, artist_id) DO UPDATE SET retrieved_at = CURRENT_TIMESTAMP""",
            [(item_id, artist_id) for artist_id in existing.values()]
        )
        