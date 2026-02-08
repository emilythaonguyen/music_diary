"""
Fetch and update metadata from MusicBrainz and Spotify.
Includes artist details, release info, track data, genres, and audio features.
"""
import asyncio
from typing import List

import aiohttp
import asyncpg
from tqdm import tqdm

from core.config import (
    DatabaseConfig,
    MusicBrainzConfig,
    SpotifyConfig,
    ValidationConfig
)
from services.musicbrainz import MusicBrainzClient
from services.spotify import SpotifyClient
from utils.database import check_metadata_refresh, parse_mb_date, chunked


class MetadataFetcher:
    """Fetch metadata from MusicBrainz and Spotify."""
    
    def __init__(self, pool: asyncpg.Pool):
        """
        Initialize fetcher.
        
        Args:
            pool: Database connection pool
        """
        self.pool = pool
        self.mb = MusicBrainzClient()
        self.spotify = SpotifyClient()
    
    # ========================================
    # musicbrainz metadata
    # ========================================
    
    async def update_artist_from_mb(
        self,
        session: aiohttp.ClientSession,
        artist_id: int,
        artist_mbid: str,
        force: bool = False
    ) -> None:
        """
        Fetch and update artist metadata from MusicBrainz.
        
        Args:
            session: aiohttp session
            artist_id: Local artist ID
            artist_mbid: MusicBrainz artist ID
        """
        # check if refresh needed
        if not force and not await check_metadata_refresh(
            self.pool, 
            "artists", 
            "artist_id", 
            artist_id, ValidationConfig.METADATA_REFRESH_DAYS
        ):
            return
        
        # fetch from api
        artist = await self.mb.get_artist(
            artist_mbid,
            includes=["aliases", "genres"]
        )
        
        if not artist:
            return
        
        async with self.pool.acquire() as conn:
            # update basic info
            await conn.execute(
                """
                UPDATE artists 
                SET sort_name = $1,
                    artist_type = $2,
                    country = $3,
                    retrieved_at = CURRENT_TIMESTAMP
                WHERE artist_id = $4
                """,
                artist.get("sort-name"),
                artist.get("type"),
                artist.get("country"),
                artist_id
            )
            
            # update aliases
            aliases = artist.get("alias-list", [])
            if aliases:
                await conn.execute(
                    "DELETE FROM artist_aliases WHERE artist_id = $1",
                    artist_id
                )
                
                alias_data = [
                    (artist_id, alias.get("name"), alias.get("primary") == "primary")
                    for alias in aliases
                    if alias.get("name")
                ]
                
                if alias_data:
                    await conn.executemany(
                        """
                        INSERT INTO artist_aliases (artist_id, alias_name, primary_alias)
                        VALUES ($1, $2, $3)
                        """,
                        alias_data
                    )
    
    async def update_release_from_mb(
        self,
        session: aiohttp.ClientSession,
        release_id: int,
        release_mbid: str,
        release_group_mbid: str,
        force: bool = False
    ) -> None:
        """
        Fetch and update release metadata from MusicBrainz.
        
        Args:
            session: aiohttp session
            release_id: Local release ID
            release_mbid: MusicBrainz release ID
            release_group_mbid: MusicBrainz release group ID
        """
        # check if refresh needed
        if not force and not await check_metadata_refresh(
            self.pool,
            "releases",
            "release_id",
            release_id,
            ValidationConfig.METADATA_REFRESH_DAYS
        ):
            return
        
        # fetch release group and release
        rg_data = await self.mb.get_release_group(
            release_group_mbid,
            includes=["genres"]
        )
        
        release_data = await self.mb.get_release(
            release_mbid,
            includes=["artist-credits", "recordings", "genres"]
        )
        
        if not rg_data or not release_data:
            return
        
        async with self.pool.acquire() as conn:
            # update release info
            await conn.execute(
                """
                UPDATE releases 
                SET release_type = $1,
                    release_date = $2,
                    country = $3,
                    retrieved_at = CURRENT_TIMESTAMP
                WHERE release_id = $4
                """,
                rg_data.get("primary-type"),
                parse_mb_date(release_data.get("date")),
                release_data.get("country"),
                release_id
            )
    
    async def update_track_from_mb(
        self,
        session: aiohttp.ClientSession,
        track_id: int,
        recording_mbid: str
    ) -> None:
        """
        Fetch and update track metadata from MusicBrainz.
        
        Args:
            session: aiohttp session
            track_id: Local track ID
            recording_mbid: MusicBrainz recording ID
        """
        # check if refresh needed
        if not await check_metadata_refresh(
            self.pool,
            "tracks",
            "track_id",
            track_id,
            ValidationConfig.METADATA_REFRESH_DAYS
        ):
            return
        
        # fetch recording
        recording = await self.mb.get_recording(
            recording_mbid,
            includes=["artist-credits"]
        )
        
        if not recording:
            return
        
        async with self.pool.acquire() as conn:
            # update track info
            length_ms = recording.get("length")
            
            await conn.execute(
                """
                UPDATE tracks 
                SET duration_ms = $1,
                    retrieved_at = CURRENT_TIMESTAMP
                WHERE track_id = $2
                """,
                int(length_ms) if length_ms else None,
                track_id
            )
    
    # ========================================
    # spotify Metadata
    # ========================================
    
    async def update_tracks_from_spotify(
        self,
        session: aiohttp.ClientSession,
        spotify_ids: List[str]
    ) -> None:
        """
        Fetch and update track metadata from Spotify.
        
        Args:
            session: aiohttp session
            spotify_ids: List of Spotify track IDs
        """
        # fetch in batches of 50
        for batch in chunked(spotify_ids, 50):
            result = await self.spotify.request(
                session,
                "/tracks",
                params={"ids": ",".join(batch)}
            )
            
            if not result or "tracks" not in result:
                continue
            
            async with self.pool.acquire() as conn:
                for track in result["tracks"]:
                    if not track:
                        continue
                    
                    # update duration and track number
                    await conn.execute(
                        """
                        UPDATE tracks 
                        SET duration_ms = $1,
                            track_number = COALESCE(track_number, $2)
                        WHERE spotify_id = $3 AND duration_ms IS NULL
                        """,
                        track.get("duration_ms"),
                        track.get("track_number"),
                        track["id"]
                    )
                    
                    # insert popularity snapshot
                    await conn.execute(
                        """
                        INSERT INTO spotify_track_popularity 
                        (track_id, popularity, snapshot_date)
                        SELECT track_id, $1, CURRENT_DATE 
                        FROM tracks 
                        WHERE spotify_id = $2
                        ON CONFLICT (track_id, snapshot_date) DO UPDATE
                        SET popularity = EXCLUDED.popularity
                        """,
                        track.get("popularity", 0),
                        track["id"]
                    )
            
            await asyncio.sleep(SpotifyConfig.REQUEST_DELAY)
    
    async def update_albums_from_spotify(
        self,
        session: aiohttp.ClientSession,
        spotify_ids: List[str]
    ) -> None:
        """
        Fetch and update album metadata from Spotify.
        
        Args:
            session: aiohttp session
            spotify_ids: List of Spotify album IDs
        """
        # fetch in batches of 20
        for batch in chunked(spotify_ids, 20):
            result = await self.spotify.request(
                session,
                "/albums",
                params={"ids": ",".join(batch)}
            )
            
            if not result or "albums" not in result:
                continue
            
            async with self.pool.acquire() as conn:
                for album in result["albums"]:
                    if not album:
                        continue
                    
                    # calculate total length
                    total_length = sum(
                        t.get("duration_ms", 0)
                        for t in album.get("tracks", {}).get("items", [])
                    )
                    
                    # update album info
                    await conn.execute(
                        """
                        UPDATE releases 
                        SET num_tracks = $1,
                            length_ms = $2
                        WHERE spotify_id = $3 
                          AND (num_tracks IS NULL OR length_ms IS NULL)
                        """,
                        album.get("total_tracks"),
                        total_length if total_length > 0 else None,
                        album["id"]
                    )
                    
                    # insert popularity snapshot
                    await conn.execute(
                        """
                        INSERT INTO spotify_release_popularity 
                        (release_id, popularity, snapshot_date)
                        SELECT release_id, $1, CURRENT_DATE 
                        FROM releases 
                        WHERE spotify_id = $2
                        ON CONFLICT (release_id, snapshot_date) DO UPDATE
                        SET popularity = EXCLUDED.popularity
                        """,
                        album.get("popularity", 0),
                        album["id"]
                    )
            
            await asyncio.sleep(SpotifyConfig.REQUEST_DELAY)
    
    async def update_artists_from_spotify(
        self,
        session: aiohttp.ClientSession,
        spotify_ids: List[str]
    ) -> None:
        """
        Fetch and update artist popularity from Spotify.
        
        Args:
            session: aiohttp session
            spotify_ids: List of Spotify artist IDs
        """
        # fetch in batches of 50
        for batch in chunked(spotify_ids, 50):
            result = await self.spotify.request(
                session,
                "/artists",
                params={"ids": ",".join(batch)}
            )
            
            if not result or "artists" not in result:
                continue
            
            async with self.pool.acquire() as conn:
                for artist in result["artists"]:
                    if not artist:
                        continue
                    
                    # insert popularity snapshot
                    await conn.execute(
                        """
                        INSERT INTO spotify_artist_popularity 
                        (artist_id, popularity, snapshot_date)
                        SELECT artist_id, $1, CURRENT_DATE 
                        FROM artists 
                        WHERE spotify_id = $2
                        ON CONFLICT (artist_id, snapshot_date) DO UPDATE
                        SET popularity = EXCLUDED.popularity
                        """,
                        artist.get("popularity", 0),
                        artist["id"]
                    )
            
            await asyncio.sleep(SpotifyConfig.REQUEST_DELAY)
    
    # ========================================
    # main processing
    # ========================================
    
    async def fetch_musicbrainz_metadata(
        self,
        session: aiohttp.ClientSession
    ) -> None:
        """Fetch all MusicBrainz metadata."""
        print("\n📊 Fetching MusicBrainz metadata...")
        
        # artists
        artists = await self.pool.fetch(
            """
            SELECT artist_id, artist_mbid 
            FROM artists 
            WHERE on_mb IS TRUE AND artist_mbid IS NOT NULL
            """
        )
        
        print(f"  Artists: {len(artists)}")
        for artist in tqdm(artists, desc="MB Artists"):
            await self.update_artist_from_mb(
                session,
                artist["artist_id"],
                artist["artist_mbid"]
            )
            await asyncio.sleep(MusicBrainzConfig.REQUEST_DELAY)
        
        # releases
        releases = await self.pool.fetch(
            """
            SELECT release_id, release_mbid, release_group_mbid
            FROM releases 
            WHERE on_mb IS TRUE 
              AND release_mbid IS NOT NULL
              AND release_group_mbid IS NOT NULL
            """
        )
        
        print(f"  Releases: {len(releases)}")
        for release in tqdm(releases, desc="MB Releases"):
            await self.update_release_from_mb(
                session,
                release["release_id"],
                release["release_mbid"],
                release["release_group_mbid"]
            )
            await asyncio.sleep(MusicBrainzConfig.REQUEST_DELAY)
        
        # tracks
        tracks = await self.pool.fetch(
            """
            SELECT track_id, recording_mbid
            FROM tracks 
            WHERE on_mb IS TRUE AND recording_mbid IS NOT NULL
            """
        )
        
        print(f"  Tracks: {len(tracks)}")
        for track in tqdm(tracks, desc="MB Tracks"):
            await self.update_track_from_mb(
                session,
                track["track_id"],
                track["recording_mbid"]
            )
            await asyncio.sleep(MusicBrainzConfig.REQUEST_DELAY)
    
    async def fetch_spotify_metadata(
        self,
        session: aiohttp.ClientSession
    ) -> None:
        """Fetch all Spotify metadata."""
        print("\n📊 Fetching Spotify metadata...")
        
        # tracks
        tracks = await self.pool.fetch(
            """
            SELECT spotify_id 
            FROM tracks 
            WHERE on_spotify IS TRUE AND duration_ms IS NULL
            """
        )
        
        if tracks:
            print(f"  Tracks: {len(tracks)}")
            spotify_ids = [t["spotify_id"] for t in tracks]
            await self.update_tracks_from_spotify(session, spotify_ids)
        
        # albums
        releases = await self.pool.fetch(
            """
            SELECT spotify_id 
            FROM releases 
            WHERE on_spotify IS TRUE 
              AND (num_tracks IS NULL OR length_ms IS NULL)
            """
        )
        
        if releases:
            print(f"  Albums: {len(releases)}")
            spotify_ids = [r["spotify_id"] for r in releases]
            await self.update_albums_from_spotify(session, spotify_ids)
        
        # artists (popularity)
        artists = await self.pool.fetch(
            """
            SELECT spotify_id 
            FROM artists 
            WHERE on_spotify IS TRUE
            """
        )
        
        if artists:
            print(f"  Artists: {len(artists)}")
            spotify_ids = [a["spotify_id"] for a in artists]
            await self.update_artists_from_spotify(session, spotify_ids)
    
    async def run(self, session: aiohttp.ClientSession) -> None:
        """Run complete metadata fetching process."""
        print("🎵 Starting metadata fetch...")
        
        await self.fetch_musicbrainz_metadata(session)
        await self.fetch_spotify_metadata(session)
        
        print("\n✅ Metadata fetch completed!")


async def main():
    """Entry point for metadata fetching."""
    pool = await asyncpg.create_pool(**DatabaseConfig.as_dict())
    
    try:
        fetcher = MetadataFetcher(pool)
        async with aiohttp.ClientSession() as session:
            await fetcher.run(session)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())