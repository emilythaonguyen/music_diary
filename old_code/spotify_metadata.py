import os
import asyncio
import aiohttp
import asyncpg
import time
from itertools import cycle
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# CONFIG
# -----------------------------
SPOTIFY_CLIENT_IDS = os.getenv("SPOTIFY_CLIENT_IDS", "").split(",")
SPOTIFY_CLIENT_SECRETS = os.getenv("SPOTIFY_CLIENT_SECRETS", "").split(",")

DB_CONFIG = {
    "database": os.getenv("DB_NAME", "music_diary"),
    "user": os.getenv("DB_USER", "emilynguyen"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_BASE_URL = "https://api.spotify.com/v1"

RETRY_LIMIT = 5
REQUEST_DELAY = 0.3

client_cycle = cycle(zip(SPOTIFY_CLIENT_IDS, SPOTIFY_CLIENT_SECRETS))
SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET = next(client_cycle)
token_data = None
token_expires = 0

# -----------------------------
# HELPERS
# -----------------------------
def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

# -----------------------------
# AUTH
# -----------------------------
async def get_spotify_token(session):
    global token_data, token_expires
    if token_data and time.time() < token_expires - 60:
        return token_data["access_token"]

    async with session.post(
        SPOTIFY_TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=aiohttp.BasicAuth(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
    ) as resp:
        token_data = await resp.json()
        token_expires = time.time() + token_data.get("expires_in", 3600)
        return token_data["access_token"]

# -----------------------------
# SAFE REQUEST
# -----------------------------
async def safe_spotify_request(session, endpoint, params=None):
    for attempt in range(RETRY_LIMIT):
        try:
            token = await get_spotify_token(session)
            headers = {"Authorization": f"Bearer {token}"}
            async with session.get(f"{SPOTIFY_BASE_URL}{endpoint}", headers=headers, params=params) as resp:
                if resp.status in (429, 500, 502, 503):
                    retry_after = int(resp.headers.get("Retry-After", 2))
                    await asyncio.sleep(retry_after + 1)
                    continue
                elif resp.status != 200:
                    text = await resp.text()
                    print(f"❗️ Spotify error {resp.status}: {text}")
                    return None
                return await resp.json()
        except Exception as e:
            await asyncio.sleep(2 ** attempt)
    print(f"❌ Max retries reached for {endpoint}")
    return None

# -----------------------------
# FETCHERS
# -----------------------------
async def fetch_tracks(session, spotify_ids):
    all_tracks = []
    for batch in chunked(spotify_ids, 50):
        data = await safe_spotify_request(session, "/tracks", params={"ids": ",".join(batch)})
        if data:
            all_tracks.extend(data.get("tracks", []))
    return all_tracks

async def fetch_albums(session, spotify_ids):
    data = await safe_spotify_request(session, "/albums", params={"ids": ",".join(spotify_ids)})
    return data.get("albums", []) if data else []

async def fetch_artists(session, spotify_ids):
    data = await safe_spotify_request(session, "/artists", params={"ids": ",".join(spotify_ids)})
    return data.get("artists", []) if data else []

# -----------------------------
# UPSERT HELPERS
# -----------------------------
async def upsert_tracks(pool, tracks):
    for t in tracks:
        if not t:
            continue
        await pool.execute("""
            UPDATE tracks
            SET duration_ms=$1, track_number=COALESCE(track_number,$2)
            WHERE spotify_id=$3 AND duration_ms IS NULL
        """, t["duration_ms"], t.get("track_number"), t["id"])
        await pool.execute("""
            INSERT INTO spotify_track_popularity (track_id, popularity, snapshot_date)
            SELECT track_id, $1, CURRENT_DATE FROM tracks WHERE spotify_id=$2
        """, t["popularity"], t["id"])

async def upsert_albums(pool, albums):
    for a in albums:
        total_length = sum([t["duration_ms"] for t in a.get("tracks", {}).get("items", []) if t.get("duration_ms")])
        await pool.execute("""
            UPDATE releases
            SET num_tracks=$1, length_ms=$2
            WHERE spotify_id=$3 AND (num_tracks IS NULL OR length_ms IS NULL)
        """, a["total_tracks"], total_length, a["id"])
        await pool.execute("""
            INSERT INTO spotify_release_popularity (release_id, popularity, snapshot_date)
            SELECT release_id, $1, CURRENT_DATE FROM releases WHERE spotify_id=$2
        """, a["popularity"], a["id"])

async def upsert_artists(pool, artists):
    for a in artists:
        await pool.execute("""
            INSERT INTO spotify_artist_popularity (artist_id, popularity, snapshot_date)
            SELECT artist_id, $1, CURRENT_DATE FROM artists WHERE spotify_id=$2
        """, a["popularity"], a["id"])

# -----------------------------
# MAIN
# -----------------------------
async def main():
    pool = await asyncpg.create_pool(**DB_CONFIG)
    async with aiohttp.ClientSession() as session:

        # --- Tracks ---
        rows = await pool.fetch("SELECT spotify_id FROM tracks WHERE on_spotify IS TRUE AND duration_ms IS NULL;")
        for batch in chunked([r["spotify_id"] for r in rows], 50):
            tracks = await fetch_tracks(session, batch)
            await upsert_tracks(pool, tracks)
            await asyncio.sleep(REQUEST_DELAY)

        # --- Releases ---
        rows = await pool.fetch("SELECT spotify_id FROM releases WHERE on_spotify IS TRUE AND (num_tracks IS NULL OR length_ms IS NULL);")
        for batch in chunked([r["spotify_id"] for r in rows], 20):
            albums = await fetch_albums(session, batch)
            await upsert_albums(pool, albums)
            await asyncio.sleep(REQUEST_DELAY)

        # --- Artists ---
        rows = await pool.fetch("SELECT spotify_id FROM artists WHERE on_spotify IS TRUE;")
        for batch in chunked([r["spotify_id"] for r in rows], 50):
            artists = await fetch_artists(session, batch)
            await upsert_artists(pool, artists)
            await asyncio.sleep(REQUEST_DELAY)

    await pool.close()
    print("✅ Spotify metadata update completed!")

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    asyncio.run(main())
