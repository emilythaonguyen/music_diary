"""
import listening history from listenbrainz into the database.
NOW WITH SUPPORT FOR UPC AND ISNI IN METADATA.
"""
import asyncio
from datetime import datetime
from typing import Optional, Dict, List

import aiohttp
import asyncpg
from tqdm import tqdm

from core.config import DatabaseConfig, ListenBrainzConfig
from utils.text import clean_artist_name, normalize_text, extract_spotify_id
from utils.database import ProgressTracker


class ListenBrainzImporter:
    """Import listens from ListenBrainz API with UPC/ISNI support."""
    
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
            "SELECT EXTRACT(EPOCH FROM MIN(listened_at))::BIGINT FROM listens"
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
                params['max_ts'] = latest_ts - 1
            
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
                        latest_ts = min(l['listened_at'] for l in batch)
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
    
    async def get_or_create_artist(self, conn, name: str, mbid: Optional[str] = None, isni: Optional[str] = None, mbids_list: List = None) -> int:
        # Fix mutable default argument pattern
        if mbids_list is None:
            mbids_list = []
            
        # 1. Standardize the name and MBID right at the start
        # This uses the logic you wanted: primary artist only if multiple are listed
        if len(mbids_list) > 1 or ',' in name:
            name = name.split(',')[0].strip()
            if mbids_list:
                mbid = mbids_list[0]
                
        clean_name = normalize_text(name)

        # 2. Try to find by MBID (The most accurate anchor)
        if mbid:
            row = await conn.fetchrow("SELECT artist_id, isni FROM artists WHERE artist_mbid = $1", mbid)
            if row:
                # Backfill ISNI if we found the artist but the record was missing it
                if isni and not row['isni']:
                    await conn.execute("UPDATE artists SET isni = $1 WHERE artist_id = $2", isni, row['artist_id'])
                return row['artist_id']
        
        # 3. Try to find by Name (or Clean Name)
        # We check for both to be safe, but prioritize the cleaned version
        artist_id = await conn.fetchval(
            "SELECT artist_id FROM artists WHERE artist_name = $1 OR artist_name = $2 LIMIT 1", 
            name, clean_name
        )
        
        if artist_id:
            # ALWAYS try to update MBID and ISNI if they are currently NULL
            # We removed the 'if isni:' guard so MBIDs actually get saved!
            await conn.execute(
                """
                UPDATE artists 
                SET artist_mbid = COALESCE(artist_mbid, $1), 
                    isni = COALESCE(isni, $2) 
                WHERE artist_id = $3
                """,
                mbid, isni, artist_id
            )
            return artist_id

        # 4. Create New Artist
        # ON CONFLICT handles the rare case where two processes try to create the same MBID at once
        return await conn.fetchval(
            """
            INSERT INTO artists (artist_name, sort_name, artist_type, artist_mbid, isni, on_mb)
            VALUES ($1, $1, 'Group', $2, $3, $4)
            ON CONFLICT (artist_mbid) 
            DO UPDATE SET 
                isni = COALESCE(artists.isni, EXCLUDED.isni),
                on_mb = TRUE
            RETURNING artist_id
            """, 
            clean_name, mbid, isni, bool(mbid)
        )

    async def get_or_create_release(self, conn, release_name: str, artist_id: int, mbid: Optional[str] = None, upc: Optional[str] = None) -> int:
        release_name = normalize_text(release_name)
        
        # 1. Try MBID First (The strongest identifier)
        if mbid:
            row = await conn.fetchrow("SELECT release_id, upc FROM releases WHERE release_mbid = $1", mbid)
            if row:
                # Backfill UPC if it's missing but we have one now
                if upc and not row['upc']:
                    await conn.execute("UPDATE releases SET upc = $1 WHERE release_id = $2", upc, row['release_id'])
                return row['release_id']
        
        # 2. Try Name + Artist (The common "stub" case)
        release_id = await conn.fetchval(
            "SELECT release_id FROM releases WHERE release_name = $1 AND primary_artist_id = $2", 
            release_name, artist_id
        )
        
        if release_id:
            # ALWAYS attempt to backfill MBID and UPC if the current record has them as NULL
            await conn.execute(
                """
                UPDATE releases 
                SET release_mbid = COALESCE(release_mbid, $1), 
                    upc = COALESCE(upc, $2),
                    on_mb = CASE WHEN $1 IS NOT NULL THEN TRUE ELSE on_mb END
                WHERE release_id = $3
                """,
                mbid, upc, release_id
            )
            return release_id

        # 3. Create New
        return await conn.fetchval(
            """
            INSERT INTO releases (release_name, primary_artist_id, release_mbid, upc, on_mb)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (release_mbid) 
            DO UPDATE SET 
                upc = COALESCE(releases.upc, EXCLUDED.upc),
                on_mb = TRUE
            RETURNING release_id
            """, 
            release_name, artist_id, mbid, upc, bool(mbid)
        )

    async def get_or_create_track(self, conn, track_name: str, release_id: int, mbid: Optional[str] = None, isrc: Optional[str] = None, spotify_id: Optional[str] = None) -> int:
        track_name = normalize_text(track_name)
        
        # 1. Try Recording MBID (Prioritize over ISRC as it's more specific to MusicBrainz)
        if mbid:
            row = await conn.fetchrow("SELECT track_id, isrc FROM tracks WHERE recording_mbid = $1", mbid)
            if row:
                if isrc and not row['isrc']:
                    await conn.execute("UPDATE tracks SET isrc = $1 WHERE track_id = $2", isrc, row['track_id'])
                return row['track_id']

        # 2. Try ISRC
        if isrc:
            row = await conn.fetchrow("SELECT track_id, recording_mbid FROM tracks WHERE isrc = $1", isrc)
            if row:
                # If found by ISRC but missing MBID, backfill it
                if mbid and not row['recording_mbid']:
                    await conn.execute("UPDATE tracks SET recording_mbid = $1 WHERE track_id = $2", mbid, row['track_id'])
                return row['track_id']
        
        # 3. Try Name + Release
        track_id = await conn.fetchval(
            "SELECT track_id FROM tracks WHERE track_name = $1 AND release_id = $2", 
            track_name, release_id
        )
        
        if track_id:
            # Backfill both MBID and ISRC if they are missing
            await conn.execute(
                """
                UPDATE tracks 
                SET recording_mbid = COALESCE(recording_mbid, $1), 
                    isrc = COALESCE(isrc, $2),
                    spotify_id = COALESCE(spotify_id, $3),
                    on_mb = CASE WHEN $1 IS NOT NULL THEN TRUE ELSE on_mb END 
                WHERE track_id = $4
                """,
                mbid, isrc, spotify_id, track_id
            )
            return track_id

        # 4. Create New
        return await conn.fetchval(
            """
            INSERT INTO tracks (track_name, release_id, recording_mbid, isrc, spotify_id, on_mb)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (recording_mbid) 
            DO UPDATE SET 
                isrc = COALESCE(tracks.isrc, EXCLUDED.isrc),
                spotify_id = COALESCE(tracks.spotify_id, EXCLUDED.spotify_id),
                on_mb = TRUE
            RETURNING track_id
            """, 
            track_name, release_id, mbid, isrc, spotify_id, bool(mbid)
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
            additional_info = metadata.get('additional_info', {}) # 1. Extract MBID info first
            mbid_mapping = metadata.get('mbid_mapping', {})
            
            # 2. Identify the primary MBID
            artist_mbids_list = mbid_mapping.get('artist_mbids', []) or additional_info.get('artist_mbids', [])
            artist_mbid = artist_mbids_list[0] if artist_mbids_list else None
            
            # 3. Now clean the name, passing the MBID to determine if we should cut at commas
            artist_name = clean_artist_name(metadata.get('artist_name', ''), mbid=artist_mbid)
            
            # 4. Extract the rest of the metadata
            release_name = metadata.get('release_name', '')
            track_name = metadata.get('track_name', '')
            listened_at = listen.get('listened_at')
            
            release_mbid = mbid_mapping.get('release_mbid') or additional_info.get('release_mbid')
            recording_mbid = mbid_mapping.get('recording_mbid') or additional_info.get('recording_mbid')
            artist_isni = additional_info.get('artist_isni')
            release_upc = additional_info.get('release_upc') or additional_info.get('barcode')
            track_isrc = additional_info.get('isrc')
            track_spotify_id = extract_spotify_id(additional_info.get('spotify_id'))
            
            # validate
            if not all([artist_name, release_name, track_name, listened_at]):
                self.stats["errors"]["missing_fields"] = self.stats["errors"].get("missing_fields", 0) + 1
                return False
            
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Use the cleaned name and known MBID to get/create the record
                    artist_id = await self.get_or_create_artist(conn, artist_name, artist_mbid, artist_isni, artist_mbids_list)
                    release_id = await self.get_or_create_release(conn, release_name, artist_id, release_mbid, release_upc)
                    track_id = await self.get_or_create_track(conn, track_name, release_id, recording_mbid, track_isrc, track_spotify_id)
                    
                    listened_at_dt = datetime.fromtimestamp(listen.get('listened_at'))
                    await conn.execute(
                        """
                        INSERT INTO listens (listened_at, track_id, release_id, artist_id) 
                        VALUES ($1, $2, $3, $4) 
                        ON CONFLICT DO NOTHING
                        """,
                        listened_at_dt, track_id, release_id, artist_id
                    )
            
            self.stats["success"] += 1
            return True
        except Exception as e:
            self.stats["errors"][str(e)] = self.stats["errors"].get(str(e), 0) + 1
            self.stats["failed"] += 1
            return False
    
    async def import_listens(self) -> None:
        """Main import process."""
        print("🎵 Starting ListenBrainz import (with UPC/ISNI/ISRC support)...")
        
        # get starting point
        last_ts = await self.get_last_listen_timestamp()
        print(f"📅 Starting from: {last_ts or 'beginning'}")
        
        # fetch listens
        async with aiohttp.ClientSession() as session:
            listens = await self.fetch_listens(session, last_ts)
        
        if not listens:
            print("ℹ️ No new listens to process")
            return
        
        print(f"\n📄 Processing {len(listens)} listens...")
        
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