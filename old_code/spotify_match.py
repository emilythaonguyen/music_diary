import os
import asyncio
import aiohttp
import asyncpg
import time
import csv
import json
import sqlite3
import re
from dotenv import load_dotenv
from tqdm.asyncio import tqdm_asyncio
from itertools import cycle
from rapidfuzz.fuzz import ratio, partial_ratio, token_sort_ratio
import pykakasi
from korean_romanizer.romanizer import Romanizer
from unidecode import unidecode
from pypinyin import lazy_pinyin

load_dotenv()

# -----------------------------
# Configuration
# -----------------------------
class Config:
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
    REQUEST_DELAY = 0.5

# -----------------------------
# Text Processing Utilities
# -----------------------------
class TextProcessor:
    _kks = pykakasi.kakasi()
    
    @staticmethod
    def detect_script(text: str) -> str:
        for ch in text:
            code = ord(ch)
            if 0xAC00 <= code <= 0xD7AF or 0x1100 <= code <= 0x11FF: return "ko"
            if 0x3040 <= code <= 0x30FF: return "ja"
            if 0x4E00 <= code <= 0x9FFF: return "zh"
        return "latin"
    
    @staticmethod
    def normalize_title(title: str, aggressive: bool = False) -> str:
        if not title: return ""
        title = title.strip().lower()
        if aggressive:
            title = re.sub(r"[\(\[\{].*?[\)\]\}]", "", title)
            title = re.sub(r'[!"#$%&\'()*+,\-./:;<=>?@[\\\]^_`{|}~]', "", title)
            title = re.sub(r"\s+", " ", title)
        return title
    
    @classmethod
    def transliterate(cls, text: str) -> str:
        if not text: return ""
        script = cls.detect_script(text)
        try:
            if script == "ja":
                return "".join(item["hepburn"] for item in cls._kks.convert(text))
            if script == "ko":
                return Romanizer(text).romanize().replace(" ", "")
            if script == "zh":
                return "".join(lazy_pinyin(text))
        except: pass
        return unidecode(text).replace(" ", "")

# -----------------------------
# Fuzzy Matching
# -----------------------------
class FuzzyMatcher:
    @staticmethod
    def compute_score(db_norm: str, db_romaji: str, candidate: str) -> float:
        cand_norm = TextProcessor.normalize_title(candidate, aggressive=True)
        cand_romaji = TextProcessor.normalize_title(TextProcessor.transliterate(candidate), aggressive=True)
        
        scores = [
            ratio(db_norm, cand_norm), ratio(db_romaji, cand_norm),
            ratio(db_norm, cand_romaji), ratio(db_romaji, cand_romaji)
        ]
        return max(scores) / 100
    
    @classmethod
    def find_best_match(cls, db_name: str, candidates: list, threshold: float = 0.90) -> str | None:
        db_norm = TextProcessor.normalize_title(db_name, aggressive=True)
        db_romaji = TextProcessor.normalize_title(TextProcessor.transliterate(db_name), aggressive=True)
        best_match, best_score = None, 0
        
        for candidate in candidates:
            score = cls.compute_score(db_norm, db_romaji, candidate.get("name", ""))
            if score > best_score:
                best_match, best_score = candidate, score
        
        return best_match.get("id") if best_match and best_score >= threshold else None

    @classmethod
    def get_ranked_matches(cls, db_name: str, candidates: list, threshold: float = 0.80) -> list:
        db_norm = TextProcessor.normalize_title(db_name, aggressive=True)
        db_romaji = TextProcessor.normalize_title(TextProcessor.transliterate(db_name), aggressive=True)
        matches = []
        for cand in candidates:
            score = cls.compute_score(db_norm, db_romaji, cand.get("name", ""))
            if score >= threshold:
                matches.append({"id": cand.get("id"), "name": cand.get("name"), "score": score, "data": cand})
        return sorted(matches, key=lambda x: x["score"], reverse=True)

# -----------------------------
# SQLite Cache Manager
# -----------------------------
class CacheManager:
    def __init__(self, db_path="spotify_cache.db"):
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

    def _create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS artist_albums (artist_id TEXT PRIMARY KEY, album_data TEXT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS album_tracks (album_id TEXT PRIMARY KEY, track_data TEXT)")
        self.conn.commit()

    def get_artist_albums(self, artist_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT album_data FROM artist_albums WHERE artist_id = ?", (artist_id,))
        row = cursor.fetchone()
        return json.loads(row[0]) if row else None

    def save_artist_albums(self, artist_id, albums):
        cursor = self.conn.cursor()
        slim_data = [{"id": a["id"], "name": a["name"]} for a in albums]
        cursor.execute("INSERT OR REPLACE INTO artist_albums VALUES (?, ?)", (artist_id, json.dumps(slim_data)))
        self.conn.commit()

    def get_album_tracks(self, album_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT track_data FROM album_tracks WHERE album_id = ?", (album_id,))
        row = cursor.fetchone()
        return json.loads(row[0]) if row else None

    def save_album_tracks(self, album_id, tracks):
        cursor = self.conn.cursor()
        slim_data = [{"id": t["id"], "name": t["name"], "artists": [a["id"] for a in t.get("artists", [])]} for t in tracks]
        cursor.execute("INSERT OR REPLACE INTO album_tracks VALUES (?, ?)", (album_id, json.dumps(slim_data)))
        self.conn.commit()

# [Previous SpotifyClient and DatabaseManager classes remain the same...]

# -----------------------------
# Spotify API Client
# -----------------------------
class SpotifyClient:
    def __init__(self):
        self.client_cycle = cycle(zip(Config.SPOTIFY_CLIENT_IDS, Config.SPOTIFY_CLIENT_SECRETS))
        self.current_id, self.current_secret = next(self.client_cycle)
        self.token_data = None
        self.token_expires = 0
        self.blocked_until = {}
        self.semaphore = asyncio.Semaphore(Config.RATE_LIMIT)
    
    async def get_token(self, session):
        """Get or refresh Spotify access token."""
        if self.token_data and time.time() < self.token_expires - 60:
            return self.token_data["access_token"]
        
        async with session.post(
            Config.SPOTIFY_TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=aiohttp.BasicAuth(self.current_id, self.current_secret),
        ) as resp:
            self.token_data = await resp.json()
            self.token_expires = time.time() + self.token_data.get("expires_in", 3600)
            return self.token_data["access_token"]
    
    def rotate_credentials(self):
        """Switch to next available API credentials."""
        for _ in range(len(Config.SPOTIFY_CLIENT_IDS)):
            cid, secret = next(self.client_cycle)
            if self.blocked_until.get(cid, 0) < time.time():
                self.current_id, self.current_secret = cid, secret
                self.token_data = None
                self.token_expires = 0
                print(f"Rotated to Spotify app -> {self.current_id[:8]}...")
                return True
        return False
    
    async def handle_rate_limit(self, retry_after: int):
        """Handle rate limiting by rotating or waiting."""
        print(f"❗️ Rate limit {retry_after//60} min for {self.current_id[:8]}...")
        self.blocked_until[self.current_id] = time.time() + retry_after
        
        if not self.rotate_credentials():
            wait_time = min(ts - time.time() for ts in self.blocked_until.values() if ts > time.time())
            print(f"⏳ All apps blocked. Waiting {int(wait_time)}s...")
            await asyncio.sleep(wait_time + 1)
        else:
            await asyncio.sleep(5)
    
    async def request(self, session, endpoint: str, params: dict = None):
        """Make rate-limited request to Spotify API."""
        async with self.semaphore:
            for attempt in range(Config.RETRY_LIMIT):
                try:
                    token = await self.get_token(session)
                    headers = {"Authorization": f"Bearer {token}"}
                    
                    async with session.get(
                        f"{Config.SPOTIFY_BASE_URL}{endpoint}",
                        headers=headers,
                        params=params
                    ) as resp:
                        if resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", 5))
                            if retry_after > 600:
                                await self.handle_rate_limit(retry_after)
                            else:
                                await asyncio.sleep(retry_after + 1)
                            continue
                        
                        if resp.status in (500, 502, 503):
                            await asyncio.sleep(2 ** attempt)
                            continue
                        
                        if resp.status == 200:
                            return await resp.json()
                        
                        print(f"❗️ Spotify API error {resp.status}: {await resp.text()}")
                        return None
                
                except Exception as e:
                    print(f"❗️ Request error: {e}. Retrying...")
                    await asyncio.sleep(2 ** attempt)
            
            print(f"❌ Max retries reached for {endpoint}")
            return None
    
    async def search_artist(self, session, name: str, limit: int = 10):
        """Search for artist on Spotify."""
        data = await self.request(session, "/search", {"q": name, "type": "artist", "limit": limit})
        return data.get("artists", {}).get("items", []) if data else []
    
    async def fetch_paginated(self, session, initial_endpoint: str, params: dict):
        """Fetch all pages of a paginated Spotify endpoint."""
        items, url = [], initial_endpoint
        
        while url:
            data = await self.request(session, url, params)
            if not data:
                break
            
            items.extend(data.get("items", []))
            next_url = data.get("next")
            url = next_url.replace(Config.SPOTIFY_BASE_URL, "") if next_url else None
            
            if url:
                await asyncio.sleep(Config.REQUEST_DELAY)
            params = {}  # Clear params after first request
        
        return items

# -----------------------------
# Database Operations
# -----------------------------
class DatabaseManager:
    def __init__(self, pool):
        self.pool = pool
        self.error_log = []
    
    def log_error(self, entity_type: str, entity_id: str, name: str, error_msg: str):
        """Log errors to both file and memory."""
        self.error_log.append([entity_type, entity_id, name, error_msg])
        print(f"❗️ {entity_type} '{name}': {error_msg}")
    
    def save_errors(self):
        """Write all errors to CSV file."""
        if not self.error_log:
            return
        
        with open("spotify_update_errors.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["entity_type", "entity_id", "name", "error"])
            writer.writerows(self.error_log)
    
    async def get_missing_artists(self):
        """Fetch artists without Spotify IDs."""
        return await self.pool.fetch("""
            SELECT artist_id, artist_name 
            FROM artists 
            WHERE spotify_id IS NULL AND (on_spotify IS TRUE OR on_spotify IS NULL)
        """)
    
    async def get_artist_aliases(self, artist_id: str):
        """Get all aliases for an artist."""
        rows = await self.pool.fetch(
            "SELECT alias_name FROM artist_aliases WHERE artist_id=$1",
            artist_id
        )
        return [r['alias_name'] for r in rows]
    
    async def get_artist_releases(self, artist_id: str, limit: int = 5):
        """Get known releases for an artist."""
        rows = await self.pool.fetch("""
            SELECT release_name
            FROM releases
            WHERE primary_artist_id = $1
            ORDER BY release_date
            LIMIT $2
        """, artist_id, limit)
        return [r["release_name"] for r in rows]
    
    async def update_artist_spotify_id(self, artist_id: str, spotify_id: str):
        """Update artist with Spotify ID."""
        await self.pool.execute("""
            UPDATE artists 
            SET spotify_id = $1, on_spotify = TRUE
            WHERE artist_id = $2
        """, spotify_id, artist_id)
    
    async def get_artists_with_spotify(self):
        """Get all artists that have Spotify IDs."""
        return await self.pool.fetch(
            "SELECT artist_id, spotify_id FROM artists WHERE spotify_id IS NOT NULL"
        )
    
    async def get_missing_releases(self):
        """Fetch releases without Spotify IDs."""
        return await self.pool.fetch("""
            SELECT release_id, release_name, primary_artist_id 
            FROM releases 
            WHERE spotify_id IS NULL AND on_spotify IS NULL
        """)
    
    async def get_artist_spotify_id(self, artist_id: str):
        """Get Spotify ID for an artist."""
        return await self.pool.fetchval(
            "SELECT spotify_id FROM artists WHERE artist_id=$1",
            artist_id
        )
    
    async def update_release_spotify_id(self, release_id: str, spotify_id: str):
        """Update release with Spotify ID."""
        await self.pool.execute(
            "UPDATE releases SET spotify_id=$1 WHERE release_id=$2",
            spotify_id, release_id
        )
    
    async def get_releases_with_spotify(self):
        """Get all releases that have Spotify IDs."""
        return await self.pool.fetch(
            "SELECT release_id, spotify_id FROM releases WHERE on_spotify = TRUE"
        )
    
    async def get_missing_tracks(self):
        """Fetch tracks without Spotify IDs."""
        return await self.pool.fetch("""
            SELECT t.track_id, t.track_name, r.spotify_id AS release_spotify_id
            FROM tracks t
            JOIN releases r ON t.release_id = r.release_id
            WHERE t.on_spotify = NULL
        """)
    
    async def update_track_spotify_id(self, track_id: str, spotify_id: str):
        """Update track with Spotify ID."""
        await self.pool.execute(
            "UPDATE tracks SET spotify_id=$1 WHERE track_id=$2",
            spotify_id, track_id
        )
        
    async def get_release_artist_spotify_ids(self, release_id: str):
        rows = await self.pool.fetch("""
            SELECT a.spotify_id
            FROM artists a
            JOIN release_artists ra ON a.artist_id = ra.artist_id
            WHERE ra.release_id = $1
                AND a.spotify_id IS NOT NULL
        """, release_id)
        return [r["spotify_id"] for r in rows]
    
    async def get_track_artist_spotify_ids(self, track_id: str):
        rows = await self.pool.fetch("""
            SELECT a.spotify_id
            FROM artists a
            JOIN track_artists ta ON a.artist_id = ta.artist_id
            WHERE ta.track_id = $1
            AND a.spotify_id IS NOT NULL
        """, track_id)
        return {r["spotify_id"] for r in rows}

# -----------------------------
# Main Processing Logic
# -----------------------------
class SpotifyUpdater:
    def __init__(self, db: DatabaseManager, spotify: SpotifyClient, cache: CacheManager):
        self.db = db
        self.spotify = spotify
        self.cache = cache

    async def validate_artist_with_releases(self, session, spotify_id: str, db_releases: list, min_score: float = 0.90):
        if not db_releases: return True
        albums = await self.spotify.request(session, f"/artists/{spotify_id}/albums", {"album_type": "album,single", "limit": 50})
        if not albums or "items" not in albums: return False
        
        db_processed = [(TextProcessor.normalize_title(t, aggressive=True), TextProcessor.normalize_title(TextProcessor.transliterate(t), aggressive=True)) for t in db_releases]
        for album in albums["items"]:
            sp_name = album.get("name")
            if not sp_name: continue
            for db_norm, db_romaji in db_processed:
                if FuzzyMatcher.compute_score(db_norm, db_romaji, sp_name) >= min_score: return True
        return False

    async def process_artists(self, session):
        artists = await self.db.get_missing_artists()
        print(f"🎨 Updating {len(artists)} artists...")
        
        sem = asyncio.Semaphore(10) # Concurrency cap
        async def update_artist(artist):
            async with sem:
                results = await self.spotify.search_artist(session, artist["artist_name"])
                if not results: return
                
                ranked = FuzzyMatcher.get_ranked_matches(artist["artist_name"], results)
                known_releases = await self.db.get_artist_releases(artist["artist_id"])
                
                for candidate in ranked:
                    if not known_releases or await self.validate_artist_with_releases(session, candidate["id"], known_releases):
                        await self.db.update_artist_spotify_id(artist["artist_id"], candidate["id"])
                        break

        await tqdm_asyncio.gather(*[update_artist(a) for a in artists], desc="Artists")

    async def fetch_all_artist_albums(self, session):
        artists = await self.db.get_artists_with_spotify()
        unique_ids = {a["spotify_id"] for a in artists if a["spotify_id"]}
        for i, sid in enumerate(unique_ids, 1):
            if self.cache.get_artist_albums(sid): continue
            albums = await self.spotify.fetch_paginated(session, f"/artists/{sid}/albums", {"album_type": "album,single,compilation", "limit": 50})
            appears_on = await self.spotify.fetch_paginated(session, f"/artists/{sid}/albums", {"album_type": "appears_on", "limit": 50})
            self.cache.save_artist_albums(sid, albums + appears_on)
            await asyncio.sleep(Config.REQUEST_DELAY)

    async def process_releases(self, session):
        releases = await self.db.get_missing_releases()
        for r in releases:
            artist_ids = await self.db.get_release_artist_spotify_ids(r["release_id"])
            if not artist_ids: continue
            for a_id in artist_ids:
                cached = self.cache.get_artist_albums(a_id)
                if not cached: continue
                match_id = FuzzyMatcher.find_best_match(r["release_name"], cached)
                if match_id:
                    await self.db.update_release_spotify_id(r["release_id"], match_id)
                    break

    async def fetch_all_album_tracks(self, session):
        releases = await self.db.get_releases_with_spotify()
        unique_ids = {r["spotify_id"] for r in releases if r["spotify_id"]}
        for sid in unique_ids:
            if self.cache.get_album_tracks(sid): continue
            tracks = await self.spotify.fetch_paginated(session, f"/albums/{sid}/tracks", {"limit": 50})
            self.cache.save_album_tracks(sid, tracks)
            await asyncio.sleep(Config.REQUEST_DELAY)

    async def process_tracks(self, session):
        tracks = await self.db.get_missing_tracks()
        for t in tracks:
            cached = self.cache.get_album_tracks(t["release_spotify_id"])
            if not cached: continue
            db_artist_ids = await self.db.get_track_artist_spotify_ids(t["track_id"])
            matches = FuzzyMatcher.get_ranked_matches(t["track_name"], cached)
            for cand in matches:
                sp_artists = set(cand["data"].get("artists", []))
                if not db_artist_ids or db_artist_ids & sp_artists:
                    await self.db.update_track_spotify_id(t["track_id"], cand["id"])
                    break

# -----------------------------
# Main Entry Point
# -----------------------------
async def main():
    pool = await asyncpg.create_pool(**Config.DB_CONFIG)
    db = DatabaseManager(pool)
    spotify = SpotifyClient()
    cache = CacheManager()
    updater = SpotifyUpdater(db, spotify, cache)
    
    async with aiohttp.ClientSession() as session:
        await updater.process_artists(session)
        await updater.fetch_all_artist_albums(session)
        await updater.process_releases(session)
        await updater.fetch_all_album_tracks(session)
        await updater.process_tracks(session)
    
    db.save_errors()
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())