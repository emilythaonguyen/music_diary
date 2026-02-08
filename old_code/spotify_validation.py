import os
import asyncio
import aiohttp
import asyncpg
import time
import csv
from dotenv import load_dotenv
from itertools import cycle
import re
from rapidfuzz import fuzz
from rapidfuzz.fuzz import ratio, partial_ratio, token_sort_ratio
import pykakasi
from korean_romanizer.romanizer import Romanizer
from unidecode import unidecode
from pypinyin import lazy_pinyin

load_dotenv()

_kks = pykakasi.kakasi()

VALIDATION_THRESHOLDS = {
    "artist": 0.90,
    "album": 0.90,
    "track": 0.88,
}

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

REQUEST_DELAY = 0.5  # seconds between requests

# -----------------------------
# global state
# -----------------------------
client_cycle = cycle(zip(SPOTIFY_CLIENT_IDS, SPOTIFY_CLIENT_SECRETS))
SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET = next(client_cycle)
token_data = None
token_expires = 0
blocked_until = {}  # client_id -> timestamp

kks = pykakasi.kakasi()

def to_romaji(text: str) -> str:
    """
    Converts Japanese text to romaji using pykakasi.
    Kanji, hiragana, katakana are all handled.
    """
    result = kks.convert(text)
    # result is a list of dicts: [{'orig': '剃', 'hira': 'てい', 'kana': 'テイ', 'hepburn': 'tei'}, ...]
    return "".join([item['hepburn'] for item in result])


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

async def get_artist_aliases(pool, artist_id):
    rows = await pool.fetch(
        "SELECT alias_name, primary_alias FROM artist_aliases WHERE artist_id=$1",
        artist_id
    )
    
    return [r['alias_name'] for r in rows]

def log_error(entity_type, entity_id, name, error_msg):
    with open("spotify_update_errors.csv", "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([entity_type, entity_id, name, error_msg])
    print(f"❗️ {entity_type} '{name}': {error_msg}")

    
def normalize_title(title: str, aggressive: bool = False) -> str:
    """
    Lowercase, strip, optionally remove ASCII punctuation/brackets.
    Keeps non-Latin characters intact.
    """
    if not title:
        return ""
    title = title.strip()
    if aggressive:
        # remove ASCII brackets only
        title = re.sub(r"[\(\[\{].*?[\)\]\}]", "", title)
        # remove ASCII punctuation only
        title = re.sub(r'[!"#$%&\'()*+,\-./:;<=>?@[\\\]^_`{|}~]', "", title)
        # normalize whitespace
        title = re.sub(r"\s+", " ", title)
    return title.lower()

def normalize_sort_name(name: str, artist_type: str) -> str:
    if not name:
        return ""
    
    if artist_type == "Person" and "," in name:
        last, first = name.split(",", 1)
        return f"{first.strip()} {last.strip()}"
    
    return name
    
def detect_script(text: str) -> str:
    has_kana = False
    has_hangul = False
    has_cjk = False

    for ch in text:
        code = ord(ch)

        if 0x3040 <= code <= 0x30FF:
            has_kana = True
        elif 0xAC00 <= code <= 0xD7AF or 0x1100 <= code <= 0x11FF:
            has_hangul = True
        elif 0x4E00 <= code <= 0x9FFF:
            has_cjk = True

    if has_hangul:
        return "ko"
    if has_kana:
        return "ja"
    if has_cjk:
        return "zh"
    return "latin"

def normalize_for_validation(text: str) -> str:
    """
    Lowercase, remove ASCII punctuation and spaces, transliterate.
    """
    if not text:
        return ""
    
    text = transliterate(text).lower()
    # Remove all ASCII punctuation and whitespace
    text = re.sub(r"[^\w]", "", text)
    return text

def transliterate(text: str) -> str:
    if not text:
        return ""

    script = detect_script(text)  # your existing function

    try:
        if script == "ja":
            result = _kks.convert(text)
            return "".join(item["hepburn"] for item in result)  # remove spaces
        if script == "ko":
            return Romanizer(text).romanize().replace(" ", "")  # unify spacing
        if script == "zh":
            return "".join(lazy_pinyin(text))  # unify spacing
    except Exception:
        pass

    return unidecode(text).replace(" ", "")

def validation_score(db_name: str, spotify_name: str) -> float:
    a = normalize_for_validation(db_name)
    b = normalize_for_validation(spotify_name)
    
    if not a or not b:
        return 0.0
    
    scores = [
        ratio(a, b),
        partial_ratio(a, b),
        token_sort_ratio(a, b)
    ]
    return max(scores) / 100

def best_validation_score(db_name, spotify_name, aliases):
    # compare DB name
    scores = [validation_score(db_name, spotify_name)]
    # compare aliases
    scores.extend(validation_score(alias, spotify_name) for alias in aliases)
    return max(scores)

async def null_spotify_id(pool, artist_id):
    await pool.execute(
        "UPDATE artists SET spotify_id = NULL WHERE artist_id = $1",
        artist_id
    )

# -----------------------
# validation scripts
#------------------------

def is_valid_spotify_id(value):
    return isinstance(value, str) and len(value) == 22 and value.isalnum()

async def validate_artists(pool, session):
    rows = await pool.fetch("""
        SELECT artist_id, artist_name, sort_name, artist_type, spotify_id
        FROM artists
        WHERE spotify_id IS NOT NULL;
    """)

    mismatches = 0
    print(f"🔍 Validating {len(rows)} artists...")

    for row in rows:
        if not is_valid_spotify_id(row["spotify_id"]):
            log_error("artist", row["artist_id"], row["artist_name"], "Malformed Spotify ID")
            mismatches += 1
            continue

        spotify = await safe_spotify_request(
            session, f"/artists/{row['spotify_id']}"
        )

        if not spotify:
            log_error("artist", row["artist_id"], row["artist_name"], "Spotify artist not found")
            mismatches += 1
            continue

        spotify_name = spotify.get("name")
        if not spotify_name:
            log_error("artist", row["artist_id"], row["artist_name"], "Spotify artist missing name field")
            mismatches += 1
            continue

        # ---- build candidate names ----
        candidates = []

        # 1. artist display name (never modified)
        candidates.append(row["artist_name"])

        # 2. sort-name (only reordered for Person)
        if row["sort_name"]:
            if row["artist_type"] == "Person":
                candidates.append(normalize_sort_name(row["sort_name"], row["artist_type"]))
            else:
                candidates.append(row["sort_name"])

        # 3. aliases
        alias_rows = await pool.fetch(
            "SELECT alias_name FROM artist_aliases WHERE artist_id=$1",
            row["artist_id"]
        )
        candidates.extend(r["alias_name"] for r in alias_rows)

        # ---- score ----
        scores = [
            validation_score(candidate, spotify_name)
            for candidate in candidates
            if candidate
        ]

        best_score = max(scores, default=0.0)

        if best_score < VALIDATION_THRESHOLDS["artist"]:
            log_error(
                "artist",
                row["artist_id"],
                row["artist_name"],
                f"Mismatch: Spotify='{spotify_name}' best_score={best_score:.2f}"
            )
            
            if best_score < 0:
                await null_spotify_id(pool, row["artist_id"])
            
            mismatches += 1
            
        await asyncio.sleep(REQUEST_DELAY)

    print(f"❌ Artist mismatches detected: {mismatches}")
        
async def main():
    pool = await asyncpg.create_pool(**DB_CONFIG)
    async with aiohttp.ClientSession() as session:
        await validate_artists(pool, session)
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())