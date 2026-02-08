"""
import listening history from listenbrainz into the database.
"""
import asyncio
from datetime import datetime
from typing import Optional, Dict, List

import aiohttp
import asyncpg
from tqdm import tqdm

from core.config import DatabaseConfig, ListenBrainzConfig
from utils.text import clean_artist_name, normalize_text
from utils.database import ProgressTracker


class ListenBrainzImporter:
    """Import listens from ListenBrainz API."""
    
    def __init__(self, pool: asyncpg.Pool):
        """
        Initialize importer.
        
        Args:
            pool: Database connection pool
        """
        self.pool = pool
        self.progress = ProgressTracker("last_max_ts.txt")
        self.stats = {"success": 0, "failed": 0, "errors": {}}
    
    async def get_last_listen_timestamp(self) -> Optional[int]:
        """Get the most recent listen timestamp from database."""
        result = await self.pool.fetchval(
            "SELECT EXTRACT(EPOCH FROM MAX(listened_at))::BIGINT FROM listens"
        )
        return result if result else None                                                   
    
    async def fetch_listens(
        self,
        session: aiohttp.ClientSession,
        last_ts: Optional[int] = None
    ) -> List[Dict]:
        """
        Fetch all new listens from ListenBrainz API.
        
        Args:
            session: aiohttp session
            last_ts: Last timestamp to fetch from
        
        Returns:
            List of listen objects
        """
        url = f"{ListenBrainzConfig.BASE_URL}/user/{ListenBrainzConfig.USERNAME}/listens"
        all_listens = []
        latest_ts = last_ts
        
        print(f"📡 Fetching listens (from ts={last_ts or 'beginning'})...")
        
        while True:
            params = {'count': ListenBrainzConfig.BATCH_SIZE}
            if latest_ts:
                params['min_ts'] = latest_ts + 1
            
            # retry logic
            for attempt in range(5):
                try:
                    async with session.get(url, params=params, timeout=15) as resp:
                        resp.raise_for_status()
                        
                        batch = (await resp.json()).get('payload', {}).get('listens', [])
                        
                        if not batch:
                            print("✅ No more new listens")
                            return all_listens
                        
                        all_listens.extend(batch)
                        latest_ts = max(l['listened_at'] for l in batch)
                        self.progress.save(latest_ts)
                        
                        print(f"  Fetched {len(batch)} (Total: {len(all_listens)})")
                        await asyncio.sleep(ListenBrainzConfig.THROTTLE)
                        break
                
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    wait_time = 2 ** attempt
                    print(f"⚠️ Error (attempt {attempt + 1}/5): {e}")
                    print(f"   Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                
                except aiohttp.ClientResponseError as e:
                    if e.status == 429:
                        print("⚠️ Rate limited. Waiting 60s...")
                        await asyncio.sleep(60)
                    else:
                        print(f"❌ HTTP {e.status}: {e}")
                        return all_listens
            else:
                print("❌ Max retries reached")
                break
        
        return all_listens
    
    async def get_or_create_artist(
        self,
        name: str,
        mbid: Optional[str] = None
    ) -> int:
        """Get or create artist in database."""
        name = normalize_text(name)
        if not name:
            raise ValueError("Artist name cannot be empty")
        
        async with self.pool.acquire() as conn:
            # try mbid first
            if mbid:
                artist_id = await conn.fetchval(
                    "SELECT artist_id FROM artists WHERE artist_mbid = $1",
                    mbid
                )
                if artist_id:
                    return artist_id
            
            # try by name
            artist_id = await conn.fetchval(
                "SELECT artist_id FROM artists WHERE artist_name = $1",
                name
            )
            if artist_id:
                return artist_id
            
            # create new
            return await conn.fetchval(
                """
                INSERT INTO artists (artist_name, sort_name, artist_type, artist_mbid)
                VALUES ($1, $1, 'Group', $2)
                ON CONFLICT (artist_mbid) DO UPDATE 
                    SET artist_name = EXCLUDED.artist_name
                RETURNING artist_id
                """,
                name, mbid
            )
    
    async def get_or_create_release(
        self,
        release_name: str,
        artist_id: int,
        mbid: Optional[str] = None
    ) -> int:
        """Get or create release in database."""
        release_name = normalize_text(release_name)
        if not release_name:
            raise ValueError("Release name cannot be empty")
        
        async with self.pool.acquire() as conn:
            # try mbid first
            if mbid:
                release_id = await conn.fetchval(
                    "SELECT release_id FROM releases WHERE release_mbid = $1",
                    mbid
                )
                if release_id:
                    return release_id
            
            # try by name + artist
            release_id = await conn.fetchval(
                """
                SELECT release_id FROM releases 
                WHERE release_name = $1 AND primary_artist_id = $2
                """,
                release_name, artist_id
            )
            if release_id:
                return release_id
            
            # create new
            return await conn.fetchval(
                """
                INSERT INTO releases (release_name, primary_artist_id, release_mbid)
                VALUES ($1, $2, $3)
                ON CONFLICT (release_mbid) DO UPDATE
                    SET release_name = EXCLUDED.release_name,
                        primary_artist_id = EXCLUDED.primary_artist_id
                RETURNING release_id
                """,
                release_name, artist_id, mbid
            )
    
    async def get_or_create_track(
        self,
        track_name: str,
        release_id: int,
        mbid: Optional[str] = None
    ) -> int:
        """Get or create track in database."""
        track_name = normalize_text(track_name)
        if not track_name:
            raise ValueError("Track name cannot be empty")
        
        async with self.pool.acquire() as conn:
            # try by name + release
            track_id = await conn.fetchval(
                """
                SELECT track_id FROM tracks 
                WHERE track_name = $1 AND release_id = $2
                """,
                track_name, release_id
            )
            if track_id:
                return track_id
            
            # create new
            return await conn.fetchval(
                """
                INSERT INTO tracks (track_name, release_id, recording_mbid)
                VALUES ($1, $2, $3)
                RETURNING track_id
                """,
                track_name, release_id, mbid
            )
    
    async def insert_listen(
        self,
        listened_at: int,
        track_id: int,
        release_id: int,
        artist_id: int
    ) -> None:
        """Insert a listen record."""
        listened_at_dt = datetime.fromtimestamp(listened_at)
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO listens (listened_at, track_id, release_id, artist_id)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (track_id, listened_at) DO NOTHING
                """,
                listened_at_dt, track_id, release_id, artist_id
            )
    
    async def process_listen(self, listen: Dict) -> bool:
        """
        Process a single listen and insert into database.
        
        Args:
            listen: Listen object from ListenBrainz
        
        Returns:
            True if successful, False otherwise
        """
        try:
            metadata = listen.get('track_metadata', {})
            
            # extract data
            artist_name = clean_artist_name(metadata.get('artist_name', ''))
            release_name = metadata.get('release_name', '')
            track_name = metadata.get('track_name', '')
            listened_at = listen.get('listened_at')
            
            # extract mbids
            mbids = metadata.get('additional_info', {})
            artist_mbid = mbids.get('artist_mbids', [None])[0] if mbids.get('artist_mbids') else None
            release_mbid = mbids.get('release_mbid')
            recording_mbid = mbids.get('recording_mbid')
            
            # validate
            if not all([artist_name, release_name, track_name, listened_at]):
                self.stats["errors"]["missing_fields"] = self.stats["errors"].get("missing_fields", 0) + 1
                return False
            
            # create entities
            artist_id = await self.get_or_create_artist(artist_name, artist_mbid)
            release_id = await self.get_or_create_release(release_name, artist_id, release_mbid)
            track_id = await self.get_or_create_track(track_name, release_id, recording_mbid)
            
            # insert listen
            await self.insert_listen(listened_at, track_id, release_id, artist_id)
            
            self.stats["success"] += 1
            return True
        
        except Exception as e:
            error_msg = str(e)
            self.stats["errors"][error_msg] = self.stats["errors"].get(error_msg, 0) + 1
            self.stats["failed"] += 1
            return False
    
    async def import_listens(self) -> None:
        """Main import process."""
        print("🎵 Starting ListenBrainz import...")
        
        # get starting point
        last_ts = await self.get_last_listen_timestamp()
        print(f"📅 Starting from: {last_ts or 'beginning'}")
        
        # fetch listens
        async with aiohttp.ClientSession() as session:
            listens = await self.fetch_listens(session, last_ts)
        
        if not listens:
            print("ℹ️ No new listens to process")
            return
        
        print(f"\n🔄 Processing {len(listens)} listens...")
        
        # process with progress bar
        for listen in tqdm(listens, desc="Importing"):
            await self.process_listen(listen)
        
        # print summary
        print("\n" + "=" * 60)
        print("✅ Import completed!")
        print(f"  Success: {self.stats['success']}")
        print(f"  Failed:  {self.stats['failed']}")
        
        if self.stats["errors"]:
            print("\n❌ Error summary:")
            for error, count in sorted(
                self.stats["errors"].items(),
                key=lambda x: -x[1]
            ):
                print(f"  • {error}: {count}")
        
        print("=" * 60)


async def main():
    """Entry point for ListenBrainz import."""
    pool = await asyncpg.create_pool(**DatabaseConfig.as_dict())
    
    try:
        importer = ListenBrainzImporter(pool)
        await importer.import_listens()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())