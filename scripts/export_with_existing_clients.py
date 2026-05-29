"""
Simplified export script that reuses your existing MusicBrainz and Last.fm clients.
Run this LOCALLY to export data to Parquet files for Databricks.
"""
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Set, Optional

import aiohttp
import pandas as pd
from tqdm import tqdm

# Add parent directory to import existing modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.musicbrainz import MusicBrainzClient
from services.lastfm import LastFMClient
from core.config import ListenBrainzConfig, MusicBrainzConfig


class DataExporter:
    """Export all data using existing API clients."""
    
    def __init__(self, output_dir: str = "./parquet_export"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize your existing clients
        self.mb_client = MusicBrainzClient()
        self.lastfm_client = LastFMClient()
        
        # Storage
        self.musicbrainz_data = []
        self.lastfm_data = []
        self.wikidata_data = []
    
    async def fetch_listenbrainz_data(self, max_ts: Optional[int] = None) -> List[Dict]:
        """Fetch ListenBrainz listens."""
        url = f"{ListenBrainzConfig.BASE_URL}/user/{ListenBrainzConfig.USERNAME}/listens"
        all_listens = []
        latest_ts = max_ts
        
        print("\n📡 Fetching ListenBrainz data...")
        
        async with aiohttp.ClientSession() as session:
            while True:
                params = {'count': ListenBrainzConfig.BATCH_SIZE}
                if latest_ts:
                    params['max_ts'] = latest_ts - 1
                
                try:
                    async with session.get(url, params=params, timeout=15) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                        batch = data.get('payload', {}).get('listens', [])
                        
                        if not batch:
                            print(f"✅ Fetched {len(all_listens)} total listens")
                            return all_listens
                        
                        all_listens.extend(batch)
                        latest_ts = min(l['listened_at'] for l in batch)
                        print(f"   Batch: {len(batch)} | Total: {len(all_listens)}")
                        await asyncio.sleep(ListenBrainzConfig.THROTTLE)
                        
                except Exception as e:
                    print(f"❌ Error fetching listens: {e}")
                    return all_listens
        
        return all_listens
    
    def extract_mbids(self, listens: List[Dict]) -> Dict[str, Set[str]]:
        """Extract unique MBIDs from listens."""
        mbids = {
            'artist_mbids': set(),
            'release_mbids': set(),
            'recording_mbids': set()
        }
        
        for listen in listens:
            metadata = listen.get('track_metadata', {})
            mbid_mapping = metadata.get('mbid_mapping', {})
            additional_info = metadata.get('additional_info', {})
            
            # Artists
            artist_list = mbid_mapping.get('artist_mbids', []) or additional_info.get('artist_mbids', [])
            mbids['artist_mbids'].update(artist_list)
            
            # Releases
            release_mbid = mbid_mapping.get('release_mbid') or additional_info.get('release_mbid')
            if release_mbid:
                mbids['release_mbids'].add(release_mbid)
            
            # Recordings
            recording_mbid = mbid_mapping.get('recording_mbid') or additional_info.get('recording_mbid')
            if recording_mbid:
                mbids['recording_mbids'].add(recording_mbid)
        
        print(f"\n📊 Extracted MBIDs:")
        print(f"   Artists: {len(mbids['artist_mbids'])}")
        print(f"   Releases: {len(mbids['release_mbids'])}")
        print(f"   Recordings: {len(mbids['recording_mbids'])}")
        
        return mbids
    
    async def fetch_musicbrainz_metadata(self, mbids: Dict[str, Set[str]]):
        """Fetch MusicBrainz metadata using your existing client."""
        print("\n📡 Fetching MusicBrainz metadata (using existing client)...")
        
        # Fetch artists
        for mbid in tqdm(list(mbids['artist_mbids']), desc="Artists"):
            data = await self.mb_client.get_artist(
                mbid,
                includes=['genres', 'tags', 'aliases', 'isnis']
            )
            if data:
                self.musicbrainz_data.append({
                    'mbid': mbid,
                    'query_term': data.get('name', ''),
                    'entity_type': 'artist',
                    'raw_payload': json.dumps(data),
                    'ingested_at': datetime.now().isoformat()
                })
            await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
        
        # Fetch releases
        for mbid in tqdm(list(mbids['release_mbids']), desc="Releases"):
            data = await self.mb_client.get_release(
                mbid,
                includes=['artists', 'recordings', 'genres', 'tags']
            )
            if data:
                self.musicbrainz_data.append({
                    'mbid': mbid,
                    'query_term': data.get('title', ''),
                    'entity_type': 'release',
                    'raw_payload': json.dumps(data),
                    'ingested_at': datetime.now().isoformat()
                })
            await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
        
        # Fetch recordings
        for mbid in tqdm(list(mbids['recording_mbids']), desc="Recordings"):
            data = await self.mb_client.get_recording(
                mbid,
                includes=['artists', 'releases', 'genres', 'tags', 'isrcs']
            )
            if data:
                self.musicbrainz_data.append({
                    'mbid': mbid,
                    'query_term': data.get('title', ''),
                    'entity_type': 'recording',
                    'raw_payload': json.dumps(data),
                    'ingested_at': datetime.now().isoformat()
                })
            await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
    
    async def fetch_lastfm_metadata(self):
        """Fetch Last.fm metadata using your existing client."""
        if not self.lastfm_client.api_key:
            print("\n⚠️  No Last.fm API key, skipping")
            return
        
        print("\n📡 Fetching Last.fm metadata (using existing client)...")
        
        # Extract artist names from MusicBrainz data
        artists = []
        for record in self.musicbrainz_data:
            if record['entity_type'] == 'artist':
                payload = json.loads(record['raw_payload'])
                artists.append({
                    'name': payload.get('name', ''),
                    'mbid': record['mbid']
                })
        
        # Fetch Last.fm data
        async with aiohttp.ClientSession() as session:
            for artist in tqdm(artists, desc="Last.fm"):
                data = await self.lastfm_client.get_artist_info(
                    session,
                    artist['name'],
                    artist['mbid']
                )
                if data:
                    self.lastfm_data.append({
                        'entity_id': artist['mbid'],
                        'entity_type': 'artist',
                        'raw_payload': json.dumps(data),
                        'ingested_at': datetime.now().isoformat()
                    })
                await asyncio.sleep(0.2)
    
    def extract_wikidata_ids(self) -> Set[tuple]:
        """Extract Wikidata IDs from MusicBrainz data."""
        wikidata_mappings = set()
        
        for record in self.musicbrainz_data:
            payload = json.loads(record['raw_payload'])
            
            # Check genres and tags for Wikidata IDs
            for item in payload.get('genres', []) + payload.get('tags', []):
                wikidata_id = item.get('wikidata-id')
                if wikidata_id:
                    wikidata_mappings.add((wikidata_id, item.get('name', '')))
        
        return wikidata_mappings
    
    async def fetch_wikidata_metadata(self, wikidata_mappings: Set[tuple]):
        """Fetch Wikidata metadata."""
        if not wikidata_mappings:
            print("\n⚠️  No Wikidata IDs found")
            return
        
        print(f"\n📡 Fetching Wikidata for {len(wikidata_mappings)} genres...")
        
        async with aiohttp.ClientSession() as session:
            for wikidata_id, genre_name in tqdm(list(wikidata_mappings), desc="Wikidata"):
                params = {
                    'action': 'wbgetentities',
                    'ids': wikidata_id,
                    'format': 'json',
                    'props': 'labels|claims'
                }
                
                try:
                    async with session.get('https://www.wikidata.org/w/api.php', params=params) as resp:
                        data = await resp.json()
                        self.wikidata_data.append({
                            'wikidata_id': wikidata_id,
                            'musicbrainz_genre': genre_name,
                            'raw_payload': json.dumps(data),
                            'ingested_at': datetime.now().isoformat()
                        })
                except Exception as e:
                    print(f"Error fetching {wikidata_id}: {e}")
                
                await asyncio.sleep(0.1)
    
    def save_to_parquet(self, listens: List[Dict]):
        """Save all data to Parquet files."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print("\n💾 Saving to Parquet files...")
        
        # Save listens
        if listens:
            bronze_records = []
            for listen in listens:
                bronze_records.append({
                    'raw_payload': json.dumps(listen),
                    'ingested_at': datetime.now().isoformat(),
                    'source_file_name': f'listenbrainz_{ListenBrainzConfig.USERNAME}_{timestamp}.json'
                })
            
            df = pd.DataFrame(bronze_records)
            output_file = self.output_dir / f'bronze_listens_{timestamp}.parquet'
            df.to_parquet(output_file, index=False, compression='snappy')
            print(f"✅ ListenBrainz: {output_file} ({len(listens)} listens)")
        
        # Save MusicBrainz
        if self.musicbrainz_data:
            df = pd.DataFrame(self.musicbrainz_data)
            output_file = self.output_dir / f'bronze_musicbrainz_{timestamp}.parquet'
            df.to_parquet(output_file, index=False, compression='snappy')
            print(f"✅ MusicBrainz: {output_file} ({len(self.musicbrainz_data)} records)")
        
        # Save Last.fm
        if self.lastfm_data:
            df = pd.DataFrame(self.lastfm_data)
            output_file = self.output_dir / f'bronze_lastfm_{timestamp}.parquet'
            df.to_parquet(output_file, index=False, compression='snappy')
            print(f"✅ Last.fm: {output_file} ({len(self.lastfm_data)} records)")
        
        # Save Wikidata
        if self.wikidata_data:
            df = pd.DataFrame(self.wikidata_data)
            output_file = self.output_dir / f'bronze_wikidata_{timestamp}.parquet'
            df.to_parquet(output_file, index=False, compression='snappy')
            print(f"✅ Wikidata: {output_file} ({len(self.wikidata_data)} records)")
    
    async def export_all(self, max_ts: Optional[int] = None):
        """Run full export pipeline."""
        print("="*60)
        print("🎵 Music Diary → Parquet Exporter")
        print("   Using your existing API clients!")
        print("="*60)
        
        # Step 1: Fetch ListenBrainz data
        listens = await self.fetch_listenbrainz_data(max_ts)
        if not listens:
            print("❌ No listens to export")
            return
        
        # Step 2: Extract MBIDs
        mbids = self.extract_mbids(listens)
        
        # Step 3: Fetch MusicBrainz metadata
        await self.fetch_musicbrainz_metadata(mbids)
        
        # Step 4: Fetch Last.fm metadata
        await self.fetch_lastfm_metadata()
        
        # Step 5: Extract and fetch Wikidata
        wikidata_mappings = self.extract_wikidata_ids()
        await self.fetch_wikidata_metadata(wikidata_mappings)
        
        # Step 6: Save everything
        self.save_to_parquet(listens)
        
        print("\n✅ Export complete!")
        print(f"   Output directory: {self.output_dir.absolute()}")
        print("\n📦 Next steps:")
        print("   1. Upload bronze_*.parquet files to Databricks")
        print("   2. Load into bronze tables")
        print("   3. Transform to silver layer")


def main():
    """Run the exporter."""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', './parquet_export')
    
    exporter = DataExporter(output_dir=OUTPUT_DIR)
    asyncio.run(exporter.export_all())


if __name__ == "__main__":
    main()
