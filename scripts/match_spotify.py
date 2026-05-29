"""
Match local entities to Spotify database.
Handles artists, albums, and tracks with fuzzy matching and local caching.
NOW WITH UPC PRIORITY FOR RELEASES AND ISNI PRIORITY FOR ARTISTS.
"""
import asyncio
import json
import sqlite3
from typing import Optional, List, Dict, Set

import aiohttp
import asyncpg
from tqdm import tqdm

from core.config import DatabaseConfig, SpotifyConfig
from services.spotify import SpotifyClient
from services.matching import EntityMatcher


class SpotifyCache:
    """SQLite cache for Spotify API responses to reduce API calls."""
    
    def __init__(self, db_path: str = "spotify_cache.db"):
        """
        Initialize cache.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.conn = sqlite3.connect(db_path)
        self._create_tables()
    
    def _create_tables(self) -> None:
        """Create cache tables."""
        cursor = self.conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS artist_albums "
            "(artist_id TEXT PRIMARY KEY, album_data TEXT)"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS album_tracks "
            "(album_id TEXT PRIMARY KEY, track_data TEXT)"
        )
        self.conn.commit()
    
    def get_artist_albums(self, artist_id: str) -> Optional[List[Dict]]:
        """Get cached albums for an artist."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT album_data FROM artist_albums WHERE artist_id = ?",
            (artist_id,)
        )
        row = cursor.fetchone()
        return json.loads(row[0]) if row else None
    
    def save_artist_albums(self, artist_id: str, albums: List[Dict]) -> None:
        """Save albums to cache."""
        cursor = self.conn.cursor()
        # Store minimal data including UPC to reduce cache size
        slim_data = [
            {
                "id": a["id"], 
                "name": a["name"],
                "upc": a.get("external_ids", {}).get("upc")  # Include UPC
            } 
            for a in albums
        ]
        cursor.execute(
            "INSERT OR REPLACE INTO artist_albums VALUES (?, ?)",
            (artist_id, json.dumps(slim_data))
        )
        self.conn.commit()
    
    def get_album_tracks(self, album_id: str) -> Optional[List[Dict]]:
        """Get cached tracks for an album."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT track_data FROM album_tracks WHERE album_id = ?",
            (album_id,)
        )
        row = cursor.fetchone()
        return json.loads(row[0]) if row else None
    
    def save_album_tracks(self, album_id: str, tracks: List[Dict]) -> None:
        """Save tracks to cache."""
        cursor = self.conn.cursor()
        # Store minimal data including ISRC
        slim_data = [
            {
                "id": t["id"],
                "name": t["name"],
                "artists": [a["id"] for a in t.get("artists", [])],
                "isrc": t.get("external_ids", {}).get("isrc")  # Include ISRC
            }
            for t in tracks
        ]
        cursor.execute(
            "INSERT OR REPLACE INTO album_tracks VALUES (?, ?)",
            (album_id, json.dumps(slim_data))
        )
        self.conn.commit()
    
    def close(self) -> None:
        """Close database connection."""
        self.conn.close()


class SpotifyMatcher:
    """Match local entities to Spotify with UPC/ISNI priority."""
    
    def __init__(self, pool: asyncpg.Pool):
        """
        Initialize matcher.
        
        Args:
            pool: Database connection pool
        """
        self.pool = pool
        self.spotify = SpotifyClient()
        self.matcher = EntityMatcher()
        self.cache = SpotifyCache()
    
    async def get_artist_releases(
        self,
        artist_id: int,
        limit: int = 10
    ) -> List[str]:
        """
        Get release names for an artist for validation.
        
        Args:
            artist_id: Local artist ID
            limit: Maximum releases to return
        
        Returns:
            List of release names
        """
        rows = await self.pool.fetch(
            """
            SELECT release_name 
            FROM releases 
            WHERE primary_artist_id = $1 
            LIMIT $2
            """,
            artist_id, limit
        )
        return [r["release_name"] for r in rows]
    
    async def process_artists(self, session: aiohttp.ClientSession) -> None:
        """Process all artists missing Spotify IDs, with ISNI priority."""
        artists = await self.pool.fetch(
            """
            SELECT artist_id, artist_name, isni
            FROM artists 
            WHERE spotify_id IS NULL AND on_spotify IS NULL
            """
        )
        
        print(f"\n🎨 Processing {len(artists)} artists (with ISNI priority)...")
        
        # process with limited concurrency
        sem = asyncio.Semaphore(10)
        
        async def process_artist(artist):
            async with sem:
                artist_isni = artist.get("isni")
                
                # PRIORITY 1: Try ISNI-based search first if available
                if artist_isni:
                    # Search with ISNI filter (if Spotify supports it)
                    # Note: Spotify API doesn't directly support ISNI search,
                    # but we can validate matches using ISNI after finding candidates
                    print(f"🔑 Artist has ISNI: {artist_isni}")
                
                # search spotify by name
                results = await self.spotify.search_artist(
                    session,
                    artist["artist_name"]
                )
                
                if not results:
                    return
                
                # get ranked matches
                ranked = self.matcher.get_ranked_matches(
                    artist["artist_name"],
                    results
                )
                
                # get known releases for validation
                known_releases = await self.get_artist_releases(artist["artist_id"])
                
                # try each candidate
                for candidate in ranked:
                    # Fetch full artist details to check ISNI if available
                    spotify_artist = await self.spotify.request(
                        session,
                        f"/artists/{candidate['id']}"
                    )
                    
                    # Check if Spotify artist has ISNI in external IDs
                    spotify_isni = None
                    if spotify_artist:
                        ext_ids = spotify_artist.get("external_ids", {})
                        spotify_isni = ext_ids.get("isni")
                    
                    # Validate with ISNI if both have it
                    if artist_isni and spotify_isni:
                        if artist_isni == spotify_isni:
                            print(f"✅ ISNI exact match for '{artist['artist_name']}'")
                            # Update database
                            async with self.pool.acquire() as conn:
                                await conn.execute(
                                    """
                                    UPDATE artists 
                                    SET spotify_id = $1, on_spotify = TRUE
                                    WHERE artist_id = $2
                                    """,
                                    candidate["id"],
                                    artist["artist_id"]
                                )
                            return
                        else:
                            # ISNI mismatch - skip this candidate
                            print(f"⚠️ ISNI mismatch for '{artist['artist_name']}' - skipping")
                            continue
                    
                    # validate with releases if available
                    if not known_releases:
                        valid = True
                    else:
                        valid = await self.matcher.validate_artist_with_releases(
                            session,
                            self.spotify,
                            candidate["id"],
                            known_releases
                        )
                    
                    if valid:
                        # update database
                        async with self.pool.acquire() as conn:
                            await conn.execute(
                                """
                                UPDATE artists 
                                SET spotify_id = $1, on_spotify = TRUE
                                WHERE artist_id = $2
                                """,
                                candidate["id"],
                                artist["artist_id"]
                            )
                        
                        print(f"🟢 Artist '{artist['artist_name']}' → {candidate['id']} (score={candidate['score']:.2f})")
                        break
        
        # process in parallel with progress bar
        tasks = [process_artist(a) for a in artists]
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Artists"):
            await task
    
    async def fetch_all_artist_albums(self, session: aiohttp.ClientSession) -> None:
        """Pre-fetch and cache all albums for artists with Spotify IDs."""
        artists = await self.pool.fetch(
            "SELECT DISTINCT spotify_id FROM artists WHERE spotify_id IS NOT NULL"
        )
        
        unique_ids = {a["spotify_id"] for a in artists if a["spotify_id"]}
        
        print(f"\n💿 Caching albums for {len(unique_ids)} artists...")
        
        for i, spotify_id in enumerate(tqdm(unique_ids, desc="Fetching albums"), 1):
            # skip if already cached
            if self.cache.get_artist_albums(spotify_id):
                continue
            
            # fetch albums and compilations
            albums = await self.spotify.fetch_paginated(
                session,
                f"/artists/{spotify_id}/albums",
                {"album_type": "album,single,compilation", "limit": 50}
            )
            
            # fetch appears_on
            appears_on = await self.spotify.fetch_paginated(
                session,
                f"/artists/{spotify_id}/albums",
                {"album_type": "appears_on", "limit": 50}
            )
            
            # cache combined results
            self.cache.save_artist_albums(spotify_id, albums + appears_on)
            
            await asyncio.sleep(SpotifyConfig.REQUEST_DELAY)
    
    async def get_release_artist_spotify_ids(self, release_id: int) -> List[str]:
        """Get Spotify IDs for all artists on a release."""
        rows = await self.pool.fetch(
            """
            SELECT a.spotify_id
            FROM artists a
            JOIN release_artists ra ON a.artist_id = ra.artist_id
            WHERE ra.release_id = $1 AND a.spotify_id IS NOT NULL
            """,
            release_id
        )
        return [r["spotify_id"] for r in rows]
    
    async def process_releases(self, session: aiohttp.ClientSession) -> None:
        """Process all releases missing Spotify IDs, with UPC priority."""
        releases = await self.pool.fetch(
            """
            SELECT release_id, release_name, primary_artist_id, upc
            FROM releases 
            WHERE spotify_id IS NULL AND on_spotify IS NULL
            """
        )
        
        print(f"\n💿 Processing {len(releases)} releases (with UPC priority)...")
        
        for release in tqdm(releases, desc="Releases"):
            release_upc = release.get("upc")
            
            # PRIORITY 1: If we have UPC, try direct UPC search
            if release_upc:
                print(f"🔑 Release has UPC: {release_upc}")
                # Note: Spotify search supports UPC via "upc:" prefix
                upc_results = await self.spotify.request(
                    session,
                    "/search",
                    params={
                        "q": f"upc:{release_upc}",
                        "type": "album",
                        "limit": 5
                    }
                )
                
                if upc_results and "albums" in upc_results and upc_results["albums"]["items"]:
                    # Found exact UPC match
                    album = upc_results["albums"]["items"][0]
                    match_id = album["id"]
                    
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE releases 
                            SET spotify_id = $1, on_spotify = TRUE
                            WHERE release_id = $2
                            """,
                            match_id,
                            release["release_id"]
                        )
                    
                    print(f"✅ UPC exact match: '{release['release_name']}' → {match_id}")
                    continue
            
            # PRIORITY 2: Fall back to name-based matching
            # get spotify ids for artists on this release
            artist_ids = await self.get_release_artist_spotify_ids(
                release["release_id"]
            )
            
            if not artist_ids:
                continue
            
            # check cache for each artist
            for artist_id in artist_ids:
                cached_albums = self.cache.get_artist_albums(artist_id)
                
                if not cached_albums:
                    continue
                
                # find best match (with UPC priority built into matcher)
                match_id = self.matcher.find_best_match(
                    release["release_name"],
                    cached_albums,
                    threshold=0.90,
                    db_upc=release_upc,
                    upc_field="upc"
                )
                
                if match_id:
                    # update database
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE releases 
                            SET spotify_id = $1, on_spotify = TRUE
                            WHERE release_id = $2
                            """,
                            match_id,
                            release["release_id"]
                        )
                    
                    print(f"🟢 Release '{release['release_name']}' → {match_id}")
                    break
    
    async def fetch_all_album_tracks(self, session: aiohttp.ClientSession) -> None:
        """Pre-fetch and cache all tracks for albums with Spotify IDs."""
        releases = await self.pool.fetch(
            "SELECT DISTINCT spotify_id FROM releases WHERE spotify_id IS NOT NULL"
        )
        
        unique_ids = {r["spotify_id"] for r in releases if r["spotify_id"]}
        
        print(f"\n🎵 Caching tracks for {len(unique_ids)} albums...")
        
        for spotify_id in tqdm(unique_ids, desc="Fetching tracks"):
            # skip if already cached
            if self.cache.get_album_tracks(spotify_id):
                continue
            
            # fetch tracks
            tracks = await self.spotify.fetch_paginated(
                session,
                f"/albums/{spotify_id}/tracks",
                {"limit": 50}
            )
            
            # cache results
            self.cache.save_album_tracks(spotify_id, tracks)
            
            await asyncio.sleep(SpotifyConfig.REQUEST_DELAY)
    
    async def get_track_artist_spotify_ids(self, track_id: int) -> Set[str]:
        """Get Spotify IDs for all artists on a track."""
        rows = await self.pool.fetch(
            """
            SELECT a.spotify_id
            FROM artists a
            JOIN track_artists ta ON a.artist_id = ta.artist_id
            WHERE ta.track_id = $1 AND a.spotify_id IS NOT NULL
            """,
            track_id
        )
        return {r["spotify_id"] for r in rows}
    
    async def process_tracks(self, session: aiohttp.ClientSession) -> None:
        """Process all tracks missing Spotify IDs, with ISRC priority."""
        tracks = await self.pool.fetch(
            """
            SELECT t.track_id, t.track_name, t.isrc, t.duration_ms, r.spotify_id AS release_spotify_id
            FROM tracks t
            JOIN releases r ON t.release_id = r.release_id
            WHERE t.spotify_id IS NULL 
              AND t.on_spotify IS NULL
              AND r.spotify_id IS NOT NULL
            """
        )
        
        print(f"\n🎵 Processing {len(tracks)} tracks (with ISRC priority)...")
        
        for track in tqdm(tracks, desc="Tracks"):
            track_isrc = track.get("isrc")
            
            # PRIORITY 1: Try ISRC-based search if available
            if track_isrc:
                print(f"🔑 Track has ISRC: {track_isrc}")
                # Spotify search supports ISRC via "isrc:" prefix
                isrc_results = await self.spotify.request(
                    session,
                    "/search",
                    params={
                        "q": f"isrc:{track_isrc}",
                        "type": "track",
                        "limit": 5
                    }
                )
                
                if isrc_results and "tracks" in isrc_results and isrc_results["tracks"]["items"]:
                    # Found exact ISRC match
                    spotify_track = isrc_results["tracks"]["items"][0]
                    match_id = spotify_track["id"]
                    
                    # Verify it's on the right album
                    album_id = spotify_track.get("album", {}).get("id")
                    if album_id == track["release_spotify_id"]:
                        async with self.pool.acquire() as conn:
                            await conn.execute(
                                """
                                UPDATE tracks 
                                SET spotify_id = $1, on_spotify = TRUE
                                WHERE track_id = $2
                                """,
                                match_id,
                                track["track_id"]
                            )
                        
                        print(f"✅ ISRC exact match: '{track['track_name']}' → {match_id}")
                        continue
            
            # PRIORITY 2: Fall back to name-based matching
            # get cached tracks for this album
            cached_tracks = self.cache.get_album_tracks(
                track["release_spotify_id"]
            )
            
            if not cached_tracks:
                continue
            
            # get artist ids for validation
            db_artist_ids = await self.get_track_artist_spotify_ids(
                track["track_id"]
            )
            
            # get ranked matches
            matches = self.matcher.get_ranked_matches(
                track["track_name"],
                cached_tracks
            )
            
            # try each match
            for candidate in matches:
                # validate by track duration
                sp_duration = candidate["data"].get("duration_ms")
                db_duration = track.get("duration_ms")
                
                if db_duration and sp_duration:
                    if abs(sp_duration - db_duration) > 3000:
                        continue
                    
                # validate artists if available
                sp_artists = set(candidate["data"].get("artists", []))
                
                if not db_artist_ids or db_artist_ids & sp_artists:
                    # update database
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE tracks 
                            SET spotify_id = $1, on_spotify = TRUE
                            WHERE track_id = $2
                            """,
                            candidate["id"],
                            track["track_id"]
                        )
                    
                    print(f"🟢 Track '{track['track_name']}' → {candidate['id']} (score={candidate['score']:.2f})")
                    break
    
    async def run(self, session: aiohttp.ClientSession) -> None:
        """Run complete Spotify matching process."""
        print("🎵 Starting Spotify matching process (with UPC/ISNI/ISRC priority)...")
        
        # phase 1: match artists (with ISNI priority)
        await self.process_artists(session)
        
        # phase 2: cache albums and match releases (with UPC priority)
        await self.fetch_all_artist_albums(session)
        await self.process_releases(session)
        
        # phase 3: cache tracks and match them (with ISRC priority)
        await self.fetch_all_album_tracks(session)
        await self.process_tracks(session)
        
        print("\n✅ Spotify matching completed!")


async def main():
    """Entry point for Spotify matching."""
    pool = await asyncpg.create_pool(**DatabaseConfig.as_dict())
    
    try:
        matcher = SpotifyMatcher(pool)
        async with aiohttp.ClientSession() as session:
            await matcher.run(session)
    finally:
        matcher.cache.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())