"""
Validate MusicBrainz matches by checking release-group relationships.
Generates CSV reports of mismatches for manual review.
NOW WITH UPC VALIDATION FOR RELEASES AND ISNI VALIDATION FOR ARTISTS.
"""
import asyncio
import csv
from datetime import datetime
from typing import Optional, Dict
from tqdm import tqdm

import aiohttp
import asyncpg

from core.config import DatabaseConfig
from services.musicbrainz import MusicBrainzClient
from services.spotify import SpotifyClient
from scripts.fetch_metadata import MetadataFetcher


class MusicBrainzValidator:
    """Validate MusicBrainz matches with UPC/ISNI priority."""
    
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
        self.spotify = self.fetcher.spotify
    
    def log_mismatch(
        self,
        entity_id: int,
        name: str,
        mbid: str,
        error_type: str,
        details: str = ""
    ) -> None:
        """
        Log a validation mismatch.
        """
        self.mismatches.append({
            "id": entity_id,
            "name": name,
            "mbid": mbid,
            "error_type": error_type,
            "details": details,
            "timestamp": datetime.now().isoformat()
        })
        print(f"⚠️ {error_type.upper()}: {name} | {details}")
        
    async def merge_artists(self, old_id: int, new_id: int):
        """Transfers all data from a duplicate artist to the survivor."""
        async with self.pool.transaction():
            # move releases
            await self.pool.execute(
                "UPDATE releases SET primary_artist_id = $1 WHERE primary_artist_id = $2", 
                new_id, old_id
            )
            
            # move listens
            await self.pool.execute(
                "UPDATE listens SET artist_id = $1 WHERE artist_id = $2", 
                new_id, old_id
            )
            
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
        """Transfers all data from a duplicate release to the survivor."""
        async with self.pool.transaction():
            # move Listens
            await self.pool.execute(
                "UPDATE listens SET release_id = $1 WHERE release_id = $2", 
                new_id, old_id
            )
            
            # move Tracks
            await self.pool.execute(
                "UPDATE tracks SET release_id = $1 WHERE release_id = $2", 
                new_id, old_id
            )
            
            # move ratings
            await self.pool.execute(
                "UPDATE ratings SET release_id = $1 WHERE release_id = $2 ON CONFLICT DO NOTHING", 
                new_id, old_id
            )
            
            # move genres
            await self.pool.execute(
                "UPDATE release_genres SET release_id = $1 WHERE release_id = $2 ON CONFLICT DO NOTHING", 
                new_id, old_id
            )
            
            # delete duplicate
            await self.pool.execute("DELETE FROM releases WHERE release_id = $1", old_id)
    
    async def validate_artists(self, session: aiohttp.ClientSession) -> int:
        """Main loop to validate all artists in the DB with ISNI validation."""
        query = """
            SELECT a.artist_id, a.artist_name, a.artist_mbid, a.spotify_id, a.isni,
                    COUNT(l.listen_id) as listen_count
            FROM artists a
            LEFT JOIN listens l on a.artist_id = l.artist_id
            WHERE a.on_mb IS TRUE
            GROUP BY a.artist_id
            ORDER BY listen_count DESC
        """
        artists = await self.pool.fetch(query)
        count = 0
        
        for artist in tqdm(artists, desc="Validating Artists", unit="artist"):
            try:
                # Fetch artist data from MusicBrainz with ISNI
                data = await self.mb_client.get_artist(
                    str(artist["artist_mbid"]), 
                    includes=["url-rels"]
                )
                
                if not data:
                    self.log_mismatch(
                        artist["artist_id"], 
                        artist["artist_name"],
                        str(artist["artist_mbid"]),
                        "api_error_or_404"
                    )
                    continue
                
                # PRIORITY: Validate ISNI if both have it
                db_isni = artist.get("isni")
                mb_isni_list = data.get("isni-list", [])
                
                if db_isni and mb_isni_list:
                    if db_isni not in mb_isni_list:
                        self.log_mismatch(
                            artist["artist_id"],
                            artist["artist_name"],
                            str(artist["artist_mbid"]),
                            "ISNI_MISMATCH",
                            f"DB ISNI: {db_isni} | MB ISNIs: {', '.join(mb_isni_list)}"
                        )
                        # This is serious - consider nulling the MBID
                        await self.pool.execute(
                            "UPDATE artists SET artist_mbid = NULL, on_mb = NULL WHERE artist_id = $1",
                            artist["artist_id"]
                        )
                        count += 1
                        continue
                    else:
                        print(f"✅ ISNI match confirmed for '{artist['artist_name']}'")
                
                # Check MBID redirects
                official_mbid = data.get("id")
                
                if official_mbid != str(artist["artist_mbid"]):
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
                            "UPDATE artists SET artist_mbid = $1 WHERE artist_id = $2",
                            official_mbid, artist["artist_id"]
                        )
                        self.log_mismatch(
                            artist["artist_id"], 
                            artist["artist_name"], 
                            official_mbid, 
                            "mbid_update", 
                            "ID was redirected"
                        )
                
                # Fill missing ISNI from MB if available
                if not db_isni and mb_isni_list:
                    await self.pool.execute(
                        "UPDATE artists SET isni = $1 WHERE artist_id = $2",
                        mb_isni_list[0], artist["artist_id"]
                    )
                    print(f"📝 Filled ISNI for '{artist['artist_name']}': {mb_isni_list[0]}")
                    count += 1
                
                # Validate Spotify ID via MusicBrainz URL relationships
                official_spid = self.mb_client.extract_spotify_id(data.get("url-relation-list", []))
                
                if official_spid and official_spid != artist["spotify_id"]:
                    if artist["spotify_id"]:
                        # Check popularity to avoid zombie profiles
                        new_data = await self.spotify.get_artist(session, official_spid)
                        current_data = await self.spotify.get_artist(session, artist["spotify_id"])
                        
                        new_pop = new_data.get("popularity", 0) if new_data else 0
                        curr_pop = current_data.get("popularity", 0) if current_data else 0
                        
                        if new_pop < curr_pop and curr_pop > 10:
                            self.log_mismatch(
                                artist["artist_id"], 
                                artist["artist_name"], 
                                official_mbid, 
                                "spotify_ignored", 
                                f"MB suggested zombie profile (Pop: {new_pop} vs Current: {curr_pop})"
                            )
                            continue
                    
                    await self.pool.execute(
                        "UPDATE artists SET spotify_id = $1 WHERE artist_id = $2",
                        official_spid, artist["artist_id"]
                    )
                    self.log_mismatch(
                        artist["artist_id"], 
                        artist["artist_name"], 
                        str(official_mbid), 
                        "spotify_correction", 
                        f"New ID: {official_spid}"
                    )
                    count += 1
                
                await asyncio.sleep(1.2)
            
            except Exception as e:
                print(f"Error processing {artist['artist_name']}: {e}")
                continue
        
        return count
        
    async def validate_releases(self, session: aiohttp.ClientSession) -> int:
        """Validate releases with UPC validation priority."""
        query = """
            SELECT r.release_id, r.release_name, r.release_mbid, r.release_group_mbid, 
                   r.upc, r.spotify_id,
                   COUNT(l.listen_id) as listen_count
            FROM releases r
            LEFT JOIN listens l on r.release_id = l.release_id
            WHERE r.on_mb is TRUE
            GROUP BY r.release_id
            ORDER BY listen_count DESC
        """
        releases = await self.pool.fetch(query)
        count = 0

        for rel in tqdm(releases, desc="Validating Releases", unit="album"):
            try:
                # Fetch release data from MusicBrainz
                data = await self.mb_client.get_release(
                    str(rel["release_mbid"]), 
                    includes=["release-groups", "artist-credits"]
                )
                
                if not data:
                    continue
                
                # PRIORITY: Validate UPC if both have it
                db_upc = rel.get("upc")
                mb_barcode = data.get("barcode")
                
                if db_upc and mb_barcode:
                    # Clean both for comparison
                    clean_db_upc = db_upc.replace("-", "").replace(" ", "")
                    clean_mb_barcode = mb_barcode.replace("-", "").replace(" ", "")
                    
                    if clean_db_upc != clean_mb_barcode:
                        self.log_mismatch(
                            rel["release_id"],
                            rel["release_name"],
                            str(rel["release_mbid"]),
                            "UPC_MISMATCH",
                            f"DB UPC: {db_upc} | MB Barcode: {mb_barcode}"
                        )
                        # This is serious - consider nulling the MBID
                        await self.pool.execute(
                            "UPDATE releases SET release_mbid = NULL, release_group_mbid = NULL, on_mb = NULL WHERE release_id = $1",
                            rel["release_id"]
                        )
                        count += 1
                        continue
                    else:
                        print(f"✅ UPC match confirmed for '{rel['release_name']}'")
                
                # Check MBID redirects
                official_release_mbid = data.get("id")
                api_rg_mbid = data.get("release-group", {}).get("id")

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
                        await self.pool.execute(
                            "UPDATE releases SET release_mbid = $1 WHERE release_id = $2", 
                            official_release_mbid, rel["release_id"]
                        )
                
                # Fill missing UPC from MB if available
                if not db_upc and mb_barcode:
                    await self.pool.execute(
                        "UPDATE releases SET upc = $1 WHERE release_id = $2",
                        mb_barcode, rel["release_id"]
                    )
                    print(f"📝 Filled UPC for '{rel['release_name']}': {mb_barcode}")
                    count += 1

                # Update release_group_mbid if changed
                if api_rg_mbid and api_rg_mbid != str(rel["release_group_mbid"]):
                    await self.pool.execute(
                        "UPDATE releases SET release_group_mbid = $1 WHERE release_id = $2", 
                        api_rg_mbid, rel["release_id"]
                    )
                    
                    # Update metadata
                    await self.fetcher.update_release_from_mb(
                        session, rel["release_id"], official_release_mbid, api_rg_mbid, force=True
                    )
                    count += 1
                
                # Validate Spotify ID via UPC if we have it
                actual_upc = db_upc or mb_barcode
                if actual_upc and rel.get("spotify_id"):
                    # Search Spotify by UPC to verify correct album
                    sp_results = await self.spotify.request(
                        session,
                        "/search",
                        params={
                            "q": f"upc:{actual_upc.replace('-', '').replace(' ', '')}",
                            "type": "album",
                            "limit": 1
                        }
                    )
                    
                    if sp_results and "albums" in sp_results and sp_results["albums"]["items"]:
                        sp_album = sp_results["albums"]["items"][0]
                        
                        if sp_album["id"] != rel["spotify_id"]:
                            self.log_mismatch(
                                rel["release_id"], 
                                rel["release_name"], 
                                str(rel["release_mbid"]),
                                "SPOTIFY_ALBUM_ID_CORRECTION",
                                f"Old: {rel['spotify_id']} | New: {sp_album['id']}"
                            )
                            await self.pool.execute(
                                "UPDATE releases SET spotify_id = $1 WHERE release_id = $2",
                                sp_album["id"], rel["release_id"]
                            )
                            count += 1
                
                await asyncio.sleep(1.2)
            
            except Exception as e:
                print(f"Error processing {rel['release_name']}: {e}")
                continue

        return count
    
    async def validate_tracks(self, session: aiohttp.ClientSession) -> int:
        """
        Validate that the local ISRC is associated with the stored recording MBID.
        """
        tracks = await self.pool.fetch(
            """
            SELECT t.track_id, t.track_name, t.recording_mbid, t.isrc, t.spotify_id
            FROM tracks t
            WHERE t.recording_mbid IS NOT NULL
                AND t.on_mb IS TRUE
            """
        )
        
        if not tracks:
            return 0
        
        count = 0
        print(f"🔍 Validating {len(tracks)} tracks against MusicBrainz ISRCs...")
        
        for track in tqdm(tracks):
            try:
                # Fetch recording data from MB (includes ISRC list)
                mb_rec = await self.mb_client.get_recording(
                    track['recording_mbid'], 
                    includes=["isrcs"]
                )
                
                if not mb_rec:
                    continue
                
                mb_isrcs = mb_rec.get('isrc-list', [])
                primary_mb_isrc = mb_isrcs[0] if mb_isrcs else None
                
                # Backfill ISRC if missing in database but present in MusicBrainz
                if not track['isrc'] and primary_mb_isrc:
                    await self.pool.execute(
                        "UPDATE tracks SET isrc = $1 WHERE track_id = $2",
                        primary_mb_isrc, track['track_id']
                    )
                    print(f"  ✨ Backfilled ISRC for '{track['track_name']}'")
                    count += 1
                
                # Use actual ISRC (from DB or just backfilled)
                actual_isrc = track['isrc'] or primary_mb_isrc
                
                # Validate Spotify ID via ISRC if we have it
                if actual_isrc and track.get('spotify_id'):
                    # Search Spotify by ISRC to verify correct track
                    sp_results = await self.spotify.request(
                        session,
                        "/search",
                        params={
                            "q": f"isrc:{actual_isrc}",
                            "type": "track",
                            "limit": 1
                        }
                    )
                    
                    if sp_results and "tracks" in sp_results and sp_results["tracks"]["items"]:
                        sp_track = sp_results["tracks"]["items"][0]
                        
                        if sp_track["id"] != track["spotify_id"]:
                            self.log_mismatch(
                                track["track_id"],
                                track["track_name"],
                                str(track["recording_mbid"]),
                                "SPOTIFY_ID_CORRECTION",
                                f"DB SP_ID: {track['spotify_id']} | Correct SP_ID: {sp_track['id']}"
                            )
                            # Auto-update if you trust the ISRC match
                            await self.pool.execute(
                                "UPDATE tracks SET spotify_id = $1 WHERE track_id = $2",
                                sp_track["id"], track["track_id"]
                            )
                            count += 1
                
                # Check for ISRC mismatches
                if track['isrc'] and mb_isrcs and track['isrc'] not in mb_isrcs:
                    self.log_mismatch(
                        track["track_id"],
                        track["track_name"],
                        str(track["recording_mbid"]),
                        "ISRC_MISMATCH",
                        f"DB ISRC: {track['isrc']} | MB ISRCs: {', '.join(mb_isrcs)}"
                    )
                    count += 1
                else:
                    if track['isrc'] and track['isrc'] in mb_isrcs:
                        print(f"✅ ISRC match confirmed for '{track['track_name']}'")
                
                await asyncio.sleep(1.5)
            
            except Exception as e:
                print(f"Error validating track {track['track_id']}: {e}")
        
        return count
    
    def save_report(self):
        """Saves all logged mismatches to CSV."""
        if not self.mismatches:
            print("\n✅ No mismatches found!")
            return
            
        keys = self.mismatches[0].keys()
        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.mismatches)
        print(f"\n📄 Report saved to {self.output_file}")

    async def run(self):
        """Run complete validation process."""
        print("🎵 Starting MusicBrainz validation (with UPC/ISNI/ISRC priority)...")
        async with aiohttp.ClientSession() as session:
            artist_count = await self.validate_artists(session)
            release_count = await self.validate_releases(session)
            track_count = await self.validate_tracks(session)
            
            print(f"\n{'='*60}")
            print("✅ Validation completed!")
            print(f"  Artist updates:  {artist_count}")
            print(f"  Release updates: {release_count}")
            print(f"  Track updates:   {track_count}")
            print(f"  Total:           {artist_count + release_count + track_count}")
            print(f"{'='*60}")
            
            self.save_report()


async def main():
    """Entry point for MusicBrainz validation."""
    pool = await asyncpg.create_pool(**DatabaseConfig.as_dict())
    try:
        validator = MusicBrainzValidator(pool)
        await validator.run()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())