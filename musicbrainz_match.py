import asyncio
import asyncpg
import musicbrainzngs
import functools
from tqdm import tqdm
from difflib import SequenceMatcher
from dotenv import load_dotenv
import os
import time


load_dotenv()

# -----------------------------
# Database config
# -----------------------------
DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

# -----------------------------
# MusicBrainz setup
# -----------------------------
mb_user_agent = os.getenv("MB_USER_AGENT")
if not mb_user_agent:
    raise ValueError("MB_USER_AGENT environment variable is missing!")
musicbrainzngs.set_useragent("ListenBrainzMatcher", "1.0", mb_user_agent)

# -----------------------------
# Global cache
# -----------------------------
artist_cache = {}
release_cache = {}

RATE_LIMIT_DELAY = 1.1

# -----------------------------
# Helpers
# -----------------------------
def similar(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

async def safe_mb_call(func, *args, retries=5, delay=2, **kwargs):
    """Async wrapper for blocking MusicBrainz calls."""
    global _last_mb_request
    
    loop = asyncio.get_event_loop()
    for attempt in range(retries):
        try:
            return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))
        except Exception as e:
            print(f"⚠️ MB request failed: {e}. Retry {attempt + 1}/{retries} in {delay}s")
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

async def run_tasks_with_progress(tasks, desc="Processing", concurrency=3):
    sem = asyncio.Semaphore(concurrency)
    
    async def sem_task(task):
        async with sem:
            return await task

    wrapped = [sem_task(t) for t in tasks]
    results = []
    for f in tqdm(asyncio.as_completed(wrapped), total=len(wrapped), desc=desc):
        results.append(await f)
    return results

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
# Artist & Release MBID updates
# -----------------------------
async def process_artist(pool, artist):
    if not artist.get("spotify_id"):
        print(f"Skipping artist '{artist['artist_name']}' (no Spotify ID)")
        return None

    search_data = await safe_mb_call(musicbrainzngs.search_artists, artist["artist_name"], limit=5)
    if not search_data or not search_data.get("artist-list"):
        print(f"⚠️ Artist '{artist['artist_name']}' not found on MB")
        return None

    for candidate in search_data["artist-list"]:
        full_artist = await safe_mb_call(
            musicbrainzngs.get_artist_by_id,
            candidate["id"],
            includes=["url-rels"]
        )
        await asyncio.sleep(RATE_LIMIT_DELAY)
        
        if not full_artist or "artist" not in full_artist:
            continue

        mb_spotify_id = extract_spotify_id(full_artist["artist"].get("url-relation-list", []))
        if mb_spotify_id != artist["spotify_id"]:
            continue

        artist_mbid = full_artist["artist"]["id"]
        
        # Check for duplicates
        existing = await pool.fetchval(
            "SELECT artist_id FROM artists WHERE artist_mbid=$1",
            artist_mbid
        )
        if existing and existing != artist["artist_id"]:
            print(f"⚠️ MBID {artist_mbid} already assigned to artist {existing}, skipping")
            return None

        await pool.execute(
            "UPDATE artists SET artist_mbid=$1 WHERE artist_id=$2",
            artist_mbid, artist["artist_id"]
        )
        print(f"✅ Artist '{artist['artist_name']}' → MBID {artist_mbid}")
        return artist_mbid

    print(f"⚠️ No MB match for artist '{artist['artist_name']}' with Spotify ID")
    return None

async def process_release(pool, release, artist_name):
    if not release.get("spotify_id"):
        print(f"Skipping release '{release['release_name']}' (no Spotify ID)")
        return None

    search_data = await safe_mb_call(
        musicbrainzngs.search_releases,
        release["release_name"],
        artist=artist_name,
        limit=5
    )
    if not search_data or not search_data.get("release-list"):
        print(f"⚠️ Release '{release['release_name']}' not found on MB")
        return None

    for candidate in search_data["release-list"]:
        full_release = await safe_mb_call(
            musicbrainzngs.get_release_by_id,
            candidate["id"],
            includes=["url-rels"]
        )
        await asyncio.sleep(RATE_LIMIT_DELAY)
        
        if not full_release or "release" not in full_release:
            continue

        mb_spotify_id = extract_spotify_id(full_release["release"].get("url-relation-list", []))
        if mb_spotify_id != release["spotify_id"]:
            continue

        release_mbid = full_release["release"]["id"]

        # Check for duplicates
        existing = await pool.fetchval(
            "SELECT release_id FROM releases WHERE release_mbid=$1",
            release_mbid
        )
        if existing and existing != release["release_id"]:
            print(f"⚠️ MBID {release_mbid} already assigned to release {existing}, skipping")
            return None

        await pool.execute(
            "UPDATE releases SET release_mbid=$1 WHERE release_id=$2",
            release_mbid, release["release_id"]
        )
        print(f"✅ Release '{release['release_name']}' → MBID {release_mbid}")
        return release_mbid

    print(f"⚠️ No MB match for release '{release['release_name']}' with Spotify ID")
    return None

# -----------------------------
# Track MBID updates
# -----------------------------
async def process_track(pool, track, artist_name, release_name, release_mbid=None):
    if not release_mbid:
        print(f"Skipping track '{track['track_name']}' (release has no MBID)")
        return

    if track.get("recording_mbid"):
        return

    track_id = track["track_id"]
    track_name = track["track_name"]

    recordings = await fetch_release_recordings(release_mbid)
    if not recordings:
        print(f"⚠️ No recordings found for track '{track_name}'")
        return

    best = max(recordings, key=lambda r: similar(track_name, r["title"]))
    score = similar(track_name, best["title"])

    if score >= 0.90:
        recording_mbid = best["id"]

        # Check for duplicates
        existing = await pool.fetchval(
            "SELECT track_id FROM tracks WHERE recording_mbid=$1",
            recording_mbid
        )
        if existing and existing != track_id:
            print(f"⚠️ MBID {recording_mbid} already assigned to track {existing}, skipping")
            return

        await pool.execute(
            "UPDATE tracks SET recording_mbid=$1 WHERE track_id=$2",
            recording_mbid, track_id
        )
        print(f"✅ Track '{track_name}' → MBID {recording_mbid} (score={score:.2f})")
    else:
        print(f"⚠️ No good MB match for track '{track_name}' (best score={score:.2f})")

async def batch_update_tracks(pool, tracks, batch_size=10):
    for start in range(0, len(tracks), batch_size):
        batch = tracks[start:start + batch_size]
        tasks = [
            process_track(pool, t, t["artist_name"], t["release_name"], t.get("release_mbid"))
            for t in batch
        ]
        await run_tasks_with_progress(tasks, desc=f"Tracks {start + 1}-{start + len(batch)}", concurrency=2)
        await asyncio.sleep(1.75)

# -----------------------------
# Main runner
# -----------------------------
async def main():
    pool = await asyncpg.create_pool(**DB_CONFIG)

    # Only fetch artists missing MBID
    artists = await pool.fetch(
        "SELECT artist_id, artist_name, spotify_id, artist_mbid "
        "FROM artists WHERE artist_mbid IS NULL;"
    )

    for artist in tqdm(artists, desc="Artists"):
        artist_mbid = await process_artist(pool, artist)
        artist_name = artist["artist_name"]

        # Only fetch releases missing MBID
        releases = await pool.fetch(
            "SELECT release_id, release_name, release_mbid, spotify_id "
            "FROM releases WHERE primary_artist_id=$1 AND release_mbid IS NULL",
            artist["artist_id"]
        )

        for release in tqdm(releases, desc=f"Releases for {artist_name}"):
            release_mbid = await process_release(pool, release, artist_name)
            if release_mbid is None:
                continue  # Skip tracks if release has no MBID

            tracks = await pool.fetch(
                """
                SELECT t.track_id, t.track_name, t.recording_mbid, t.spotify_id,
                    r.release_name, r.release_mbid, a.artist_name
                FROM tracks t
                JOIN releases r ON t.release_id = r.release_id
                JOIN artists a ON r.primary_artist_id = a.artist_id
                WHERE t.release_id=$1 AND t.recording_mbid IS NULL
                """,
                release["release_id"]
            )

            if tracks: 
                await batch_update_tracks(pool, tracks, batch_size=10)

    await pool.close()
    print("🎉 MusicBrainz update completed safely!")

if __name__ == "__main__":
    asyncio.run(main())