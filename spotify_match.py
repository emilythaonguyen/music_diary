import os
import asyncio
import aiohttp
import asyncpg
import time
import string
import csv
import json
from dotenv import load_dotenv
from difflib import SequenceMatcher
from tqdm.asyncio import tqdm_asyncio
from itertools import cycle
import re
import json

load_dotenv()

# -----------------------------
# config
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

RATE_LIMIT = 5
RETRY_LIMIT = 5

ARTIST_CACHE_FILE = "artist_albums_cache.json"
TRACK_CACHE_FILE = "album_tracks_cache.json"

ALBUM_BATCH_SIZE = 20
TRACK_BATCH_SIZE = 20
REQUEST_DELAY = 0.5  # seconds between requests

# -----------------------------
# global state
# -----------------------------
client_cycle = cycle(zip(SPOTIFY_CLIENT_IDS, SPOTIFY_CLIENT_SECRETS))
SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET = next(client_cycle)
token_data = None
token_expires = 0
blocked_until = {}  # client_id -> timestamp

artist_albums_cache = {}
album_tracks_cache = {}

# -----------------------------
# utility functions
# -----------------------------
def log_error(entity_type, entity_id, name, error_msg):
    with open("spotify_update_errors.csv", "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([entity_type, entity_id, name, error_msg])
    print(f"❗️ {entity_type} '{name}': {error_msg}")

def normalize_title(title, aggressive=False):
    if not title:
        return ""
    title = title.lower().strip()
    if aggressive:
        # remove brackets and their contents: (official), [deluxe], {live}, etc.
        title = re.sub(r"[\(\[\{].*?[\)\]\}]", "", title)
        # remove punctuation
        title = re.sub(r"[^\w\s]", "", title)
        # normalize whitespace
        title = re.sub(r"\s+", " ", title)
    return title

def similar(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def is_valid_spotify_id(value):
    return isinstance(value, str) and len(value) == 22 and value.isalnum()

def save_cache(filename, cache):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(cache, f)
    print(f"💾 Saved cache ({len(cache)} items) -> {filename}")

def load_cache(filename):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            cache = json.load(f)
        print(f"💾 Loaded cache ({len(cache)} items) <- {filename}")
        return cache
    return {}

# -----------------------------
# spotify auth & rotation
# -----------------------------
async def get_spotify_token(session):
    global token_data, token_expires, SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
    if token_data and time.time() < token_expires - 60:
        return token_data["access_token"]

    async with session.post(
        SPOTIFY_TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=aiohttp.BasicAuth(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
    ) as resp:
        token_data = await resp.json()
        token_expires = time.time() + token_data.get("expires_in", 3600)
        return token_data["access_token"]

def rotate_credentials():
    global SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, token_data, token_expires
    for _ in range(len(SPOTIFY_CLIENT_IDS)):
        cid, secret = next(client_cycle)
        if blocked_until.get(cid, 0) < time.time():
            SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET = cid, secret
            token_data = None
            token_expires = 0
            print(f"Rotated to available Spotify app -> {SPOTIFY_CLIENT_ID[:8]}...")
            return True
    return False

async def handle_retry_after(retry_after):
    global blocked_until
    print(f"❗️ Global cooldown {retry_after//60} min detected for {SPOTIFY_CLIENT_ID[:8]}...")
    blocked_until[SPOTIFY_CLIENT_ID] = time.time() + retry_after
    rotated = rotate_credentials()
    if rotated:
        await asyncio.sleep(5)
    else:
        wait_time = min(ts - time.time() for ts in blocked_until.values() if ts > time.time())
        print(f"⏳ All apps blocked. Waiting {int(wait_time)}s...")
        await asyncio.sleep(wait_time + 1)

# -----------------------------
# safe spotify request
# -----------------------------
safe_spotify_request_semaphore = asyncio.Semaphore(RATE_LIMIT)

async def safe_spotify_request(session, endpoint, params=None):
    async with safe_spotify_request_semaphore:
        retries = 0
        backoff = 1
        while retries < RETRY_LIMIT:
            try:
                token = await get_spotify_token(session)
                headers = {"Authorization": f"Bearer {token}"}
                async with session.get(f"{SPOTIFY_BASE_URL}{endpoint}", headers=headers, params=params) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        if retry_after > 600:
                            await handle_retry_after(retry_after)
                            continue
                        else:
                            await asyncio.sleep(retry_after + 1)
                    elif resp.status in (500, 502, 503):
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 60)
                    elif resp.status == 400:
                        text = await resp.text()
                        print(f"❗️ Spotify API error 400: {text}")
                        return None
                    elif resp.status != 200:
                        text = await resp.text()
                        print(f"❗️ Spotify API error {resp.status}: {text}")
                        return None
                    else:
                        return await resp.json()
            except Exception as e:
                print(f"❗️ Request error: {e}. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            retries += 1
        print(f"❌ Max retries reached for {endpoint}")
        return None

# -----------------------------
# spotify helpers
# -----------------------------
async def search_artist(session, name, limit=10):
    """
    Searches Spotify for an artist, returns list of artist dicts.
    Increases search limit for international artists.
    """
    data = await safe_spotify_request(session, "/search", {"q": name, "type": "artist", "limit": 5})
    return data.get("artists", {}).get("items", []) if data else []

async def fetch_artist_albums(session, artist_id):
    albums, url = [], f"/artists/{artist_id}/albums"
    params = {"album_type": "album,single,compilation", "limit": 50}
    while url:
        data = await safe_spotify_request(session, url, params)
        if not data:
            break
        albums.extend(data.get("items", []))
        next_url = data.get("next")
        url = next_url.replace(SPOTIFY_BASE_URL, "") if next_url else None
        await asyncio.sleep(REQUEST_DELAY)
    return albums

async def fetch_album_tracks(session, album_id):
    tracks, url = [], f"/albums/{album_id}/tracks"
    params = {"limit": 50}
    while url:
        data = await safe_spotify_request(session, url, params)
        if not data:
            break
        tracks.extend(data.get("items", []))
        next_url = data.get("next")
        url = next_url.replace(SPOTIFY_BASE_URL, "") if next_url else None
        await asyncio.sleep(REQUEST_DELAY)
    return tracks

# -----------------------------
# fuzzy matching
# -----------------------------
def fuzzy_match_artist(db_name, spotify_artists):
    """
    Returns the Spotify ID of the best matching artist based on fuzzy string similarity.
    Adjusts threshold for short names.
    """
    db_norm = normalize_title(db_name)
    best = max(
        spotify_artists,
        key=lambda a: similar(db_norm, normalize_title(a.get("name", ""))),
        default=None
    )
    if not best:
        return None

    similarity_score = similar(db_norm, normalize_title(best["name"]))
    threshold = 0.8
    if len(db_norm) <= 3:
        threshold = 0.6  # allow lower similarity for short names

    return best["id"] if similarity_score >= threshold else None

def fuzzy_match_release(db_title, spotify_albums):
    db_norm = normalize_title(db_title, aggressive=True)
    best = None
    best_score = 0
    for a in spotify_albums:
        try:
            norm_name = normalize_title(a["name"], aggressive=True)
            score = similar(db_norm, norm_name)
            if score > best_score:
                best = a
                best_score = score
        except KeyError:
            print(f"⚠️ Album missing 'name' key: {a}")
    if best and best_score > 0.85:
        return best["id"]
    return None

def fuzzy_match_track(db_title, spotify_tracks):
    db_norm = normalize_title(db_title, aggressive=True)  # for_track no longer exists
    best = None
    best_score = 0
    for a in spotify_tracks:
        try:
            norm_name = normalize_title(a["name"], aggressive=True)
            score = similar(db_norm, norm_name)
            if score > best_score:
                best = a
                best_score = score
        except KeyError:
            print(f"⚠️ Track missing 'name' key: {a}")
    if best and best_score > 0.85:
        return best["id"]
    return None


# -----------------------------
# throttled fetching
# -----------------------------
async def fetch_artist_albums_safe(session, artists_with_spotify):
    """
    Fetch albums for each unique Spotify ID only once, even if multiple artists share it.
    """
    global artist_albums_cache
    # Get unique Spotify IDs
    unique_spotify_ids = {a["spotify_id"] for a in artists_with_spotify if is_valid_spotify_id(a["spotify_id"])}
    total = len(unique_spotify_ids)
    for i, spotify_id in enumerate(unique_spotify_ids, 1):
        if spotify_id in artist_albums_cache:
            continue
        print(f"📀 Fetching albums {i}/{total} for Spotify ID {spotify_id}")
        albums = await fetch_artist_albums(session, spotify_id)
        artist_albums_cache[spotify_id] = albums
        if len(artist_albums_cache) % 50 == 0:
            save_cache(ARTIST_CACHE_FILE, artist_albums_cache)
        await asyncio.sleep(REQUEST_DELAY)
    save_cache(ARTIST_CACHE_FILE, artist_albums_cache)


async def fetch_album_tracks_safe(session, releases_with_spotify):
    """
    Fetch tracks for each unique album Spotify ID only once.
    """
    global album_tracks_cache
    unique_album_ids = {r["spotify_id"] for r in releases_with_spotify if is_valid_spotify_id(r["spotify_id"])}
    total = len(unique_album_ids)
    for i, spotify_id in enumerate(unique_album_ids, 1):
        if spotify_id in album_tracks_cache:
            continue
        print(f"🎵 Fetching tracks {i}/{total} for album Spotify ID {spotify_id}")
        tracks = await fetch_album_tracks(session, spotify_id)
        album_tracks_cache[spotify_id] = tracks
        if len(album_tracks_cache) % 50 == 0:
            save_cache(TRACK_CACHE_FILE, album_tracks_cache)
        await asyncio.sleep(REQUEST_DELAY)
    save_cache(TRACK_CACHE_FILE, album_tracks_cache)

# -----------------------------
# main async logic
# -----------------------------
async def main():
    global artist_albums_cache, album_tracks_cache
    artist_albums_cache = load_cache(ARTIST_CACHE_FILE)
    album_tracks_cache = load_cache(TRACK_CACHE_FILE)

    pool = await asyncpg.create_pool(**DB_CONFIG)
    async with aiohttp.ClientSession() as session:

        # --- ARTISTS ---
        artists_missing = await pool.fetch("SELECT artist_id, artist_name FROM artists WHERE spotify_id IS NULL;")
        print(f"🎨 Updating {len(artists_missing)} artists...")
        artist_success, artist_fail = 0, 0

        async def update_artist(artist):
            """
            Update Spotify ID for a single artist using fuzzy_match_artist.
            """
            nonlocal artist_success, artist_fail

            results = await search_artist(session, artist["artist_name"])
            if not results:
                log_error("artist", artist["artist_id"], artist["artist_name"], "No search results")
                artist_fail += 1
                return

            spotify_id = fuzzy_match_artist(artist["artist_name"], results)
            if spotify_id:
                await pool.execute(
                    "UPDATE artists SET spotify_id=$1 WHERE artist_id=$2",
                    spotify_id, artist["artist_id"]
                )
                print(f"🟢 {artist['artist_name']} → Spotify ID {spotify_id}")
                artist_success += 1
            else:
                log_error(
                    "artist",
                    artist["artist_id"],
                    artist["artist_name"],
                    "No suitable match found"
                )
                artist_fail += 1

        await tqdm_asyncio.gather(*[update_artist(a) for a in artists_missing], desc="Artists")
        print(f"🟢 Artists updated: {artist_success}, 🔴 Unmatched: {artist_fail}")

        # --- ALBUMS ---
        artists_with_spotify = await pool.fetch("SELECT artist_id, spotify_id FROM artists WHERE spotify_id IS NOT NULL;")
        await fetch_artist_albums_safe(session, artists_with_spotify)

        # --- RELEASES ---
        releases_missing = await pool.fetch("SELECT release_id, release_name, primary_artist_id FROM releases WHERE spotify_id IS NULL;")
        release_success, release_fail = 0, 0
        for release in releases_missing:
            artist_spotify_id = await pool.fetchval(
                "SELECT spotify_id FROM artists WHERE artist_id=$1",
                release["primary_artist_id"]
            )
            if artist_spotify_id and artist_spotify_id in artist_albums_cache:
                spotify_id = fuzzy_match_release(release["release_name"], artist_albums_cache[artist_spotify_id])
                if spotify_id:
                    await pool.execute("UPDATE releases SET spotify_id=$1 WHERE release_id=$2",
                                       spotify_id, release["release_id"])
                    release_success += 1
                    continue
            log_error("release", release["release_id"], release["release_name"], "No match found")
            release_fail += 1
        print(f"🟢 Releases updated: {release_success}, 🔴 Unmatched: {release_fail}")

        # --- TRACKS ---
        releases_with_spotify = await pool.fetch("SELECT release_id, spotify_id FROM releases WHERE spotify_id IS NOT NULL;")
        await fetch_album_tracks_safe(session, releases_with_spotify)

        tracks_missing = await pool.fetch("""
            SELECT t.track_id, t.track_name, r.spotify_id AS release_spotify_id
            FROM tracks t
            JOIN releases r ON t.release_id = r.release_id
            WHERE t.spotify_id IS NULL;
        """)
        track_success, track_fail = 0, 0
        for track in tracks_missing:
            release_spotify_id = track["release_spotify_id"]
            if not release_spotify_id or release_spotify_id not in album_tracks_cache:
                continue
            spotify_id = fuzzy_match_track(track["track_name"], album_tracks_cache[release_spotify_id])
            if spotify_id:
                await pool.execute("UPDATE tracks SET spotify_id=$1 WHERE track_id=$2",
                                   spotify_id, track["track_id"])
                track_success += 1
            else:
                log_error("track", track["track_id"], track["track_name"], "No match found")
                track_fail += 1
        print(f"🟢 Tracks updated: {track_success}, 🔴 Unmatched: {track_fail}")

    await pool.close()
    print("🐸 Spotify update completed!")

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    asyncio.run(main())
