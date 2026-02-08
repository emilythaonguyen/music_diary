"""
Validate MusicBrainz matches by checking release-group relationships.
Generates CSV reports of mismatches for manual review.
"""
import asyncio
import csv
from typing import Optional, Dict

import aiohttp
import asyncpg

from core.config import DatabaseConfig, MusicBrainzConfig
from services.musicbrainz import MusicBrainzClient
from scripts.fetch_metadata import MetadataFetcher


class MusicBrainzValidator:
    """Validate MusicBrainz matches."""
    
    def __init__(self, pool: asyncpg.Pool, output_file: str = "musicbrainz_mismatches.csv"):
        """
        Initialize validator.
        
        Args:
            pool: Database connection pool
            output_file: Path to CSV output file
        """
        self.pool = pool
        self.output_file = output_file
        self.mismatches = []
        self.rg_cache: Dict[str, Optional[str]] = {}
        self.mb_client = MusicBrainzClient()
        self.fetcher = MetadataFetcher(pool)
    
    def log_mismatch(
        self,
        release_id: int,
        release_name: str,
        release_mbid: str,
        group_mbid: str,
        error_type: str
    ) -> None:
        """
        Log a validation mismatch.
        
        Args:
            release_id: Database ID
            release_name: Release name
            release_mbid: MusicBrainz release ID
            group_mbid: MusicBrainz release group ID
            error_type: Type of error
        """
        self.mismatches.append({
            "release_id": release_id,
            "release_name": release_name,
            "release_mbid": release_mbid,
            "group_mbid": group_mbid,
            "error_type": error_type
        })
        print(f"⚠️ Release '{release_name}': {error_type}")
        
    async def merge_artists(self, old_id: int, new_id: int):
        """Transfers all data from a duplicate artist to the survivor."""
        async with self.pool.transaction():
            # move releases
            await self.pool.execute("UPDATE releases SET primary_artist_id = $1 WHERE primary_artist_id = $2", new_id, old_id)
            
            # move listens
            await self.pool.execute("UPDATE listens SET artist_id = $1 WHERE artist_id = $2", new_id, old_id)
            
            # move genres
            await self.pool.execute(
                "UPDATE artist_genres SET artist_id = $1 WHERE artist_id = $2 ON CONFLICT DO NOTHING", 
                new_id, old_id
            )

            # move aliases
            await self.pool.execute(
                "UPDATE artist_aliases SET artist_id = $1 WHERE artist_id = $2 ON CONFLICT DO NOTHING", 
                new_id, old_id
            )

            # delete duplicate
            await self.pool.execute("DELETE FROM artists WHERE artist_id = $1", old_id)
    
    async def merge_releases(self, old_id: int, new_id: int):
        async with self.pool.transaction():
            # move Listens
            await self.pool.execute("UPDATE listens SET release_id = $1 WHERE release_id = $2", new_id, old_id)
            # move Tracks
            await self.pool.execute("UPDATE tracks SET release_id = $1 WHERE release_id = $2", new_id, old_id)
            # move ratings
            await self.pool.execute("UPDATE ratings SET release_id = $1 WHERE release_id = $2 ON CONFLICT DO NOTHING", new_id, old_id)
            # move genres
            await self.pool.execute("UPDATE release_genres SET release_id = $1 WHERE release_id = $2 ON CONFLICT DO NOTHING", new_id, old_id)
            
            # delete duplicate
            await self.pool.execute("DELETE FROM releases WHERE release_id = $1", old_id)
    
    async def validate_artists(self) -> int:
        """Main loop to validate all artists in the DB."""
        artists = await self.pool.fetch("SELECT artist_id, artist_name, artist_mbid, spotify_id FROM artists WHERE on_mb IS TRUE")
        count = 0
        
        for artist in artists:
            data = await self.mb_client.get_artist(str(artist["artist_mbid"]), includes=["url-rels"])
            
            if not data:
                self.log_mismatch(artist["artist_id"], 
                                  artist["artist_name"],
                                  str(artist["artist_mbid"]),
                                  "api_error_or_404")
                continue
            
            # check mbid
            official_mbid = data.get("id")
            
            if official_mbid != str(artist["artist_mbid"]):
                # 1. Check if the "True" MBID already exists in our local DB
                existing_artist = await self.pool.fetchrow(
                    "SELECT artist_id FROM artists WHERE artist_mbid = $1", 
                    official_mbid
                )

                if existing_artist:
                    print(f"🔀 MERGE DETECTED: {artist['artist_name']} -> ID {existing_artist['artist_id']}")
                    await self.merge_artists(artist["artist_id"], existing_artist["artist_id"])
                    count += 1
                    continue
                
                else:
                    await self.pool.execute(
                        "UPDATE artists SET artist_mbid = $1 WHERE artist_id = $2",official_mbid, artist["artist_id"])
                    self.log_mismatch(artist["artist_id"], artist["artist_name"], official_mbid, "mbid_update", "ID was redirected")
            
            official_spid = self.mb_client.extract_spotify_id(data.get("url-relation-list", []))
            
            # spotify id mismatch
            if official_spid and official_spid != artist["spotify_id"]:
                await self.pool.execute(
                    "UPDATE artists SET spotify_id = $1 WHERE artist_id = $2",
                    official_spid, artist["artist_id"]
                )
                self.log_mismatch(artist["artist_id"], artist["artist_name"], str(artist["artist_mbid"]), "spotify_correction", f"New ID: {official_spid}")
                count += 1
        return count
        
    async def validate_releases(self, session: aiohttp.ClientSession) -> int:
        releases = await self.pool.fetch("SELECT release_id, release_name, release_mbid, release_group_mbid FROM releases WHERE on_mb IS TRUE")
        count = 0

        for rel in releases:
            data = await self.mb_client.get_release(str(rel["release_mbid"]), includes=["release-groups", "artist-credits"])
            if not data: continue

            official_release_mbid = data.get("id")
            api_rg_mbid = data.get("release-group", {}).get("id")

            # 1. HANDLE RELEASE MERGE (If the Release MBID changed)
            if official_release_mbid != str(rel["release_mbid"]):
                existing_rel = await self.pool.fetchrow(
                    "SELECT release_id FROM releases WHERE release_mbid = $1", 
                    official_release_mbid
                )
                
                if existing_rel:
                    print(f"💿 MERGE RELEASES: {rel['release_name']} -> Existing ID {existing_rel['release_id']}")
                    await self.merge_releases(rel["release_id"], existing_rel["release_id"])
                    count += 1
                    continue 
                else:
                    # Just an ID update
                    await self.pool.execute("UPDATE releases SET release_mbid = $1 WHERE release_id = $2", official_release_mbid, rel["release_id"])

            # update release_group_mbid only
            if api_rg_mbid != str(rel["release_group_mbid"]):
                await self.pool.execute("UPDATE releases SET release_group_mbid = $1 WHERE release_id = $2", api_rg_mbid, rel["release_id"])
                
                # update new metadata
                await self.fetcher.update_release_from_mb(
                    session, rel["release_id"], official_release_mbid, api_rg_mbid, force=True
                )
                count += 1
        return count
    
    def save_report(self):
        """Saves all logged mismatches to CSV."""
        if not self.mismatches: return
        keys = self.mismatches[0].keys()
        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.mismatches)
        print(f"\n📄 Report saved to {self.output_file}")

    async def run(self):
        async with aiohttp.ClientSession() as session:
            await self.validate_artists()
            await self.validate_releases(session)
            self.save_report()

async def main():
    pool = await asyncpg.create_pool(**DatabaseConfig.as_dict())
    try:
        validator = MusicBrainzValidator(pool)
        await validator.run()
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())