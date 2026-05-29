"""
Export ListenBrainz data to Parquet - REUSES your existing clients!
This version leverages your core.config and existing API infrastructure.
"""
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import aiohttp
import pandas as pd
from tqdm import tqdm

# Add parent directory to import your modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import ListenBrainzConfig
from services.musicbrainz import MusicBrainzClient
from services.lastfm import LastFMClient


class ListenBrainzExporter:
    """Export ListenBrainz data using existing infrastructure."""
    
    def __init__(self, output_dir: str = "./output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.username = ListenBrainzConfig.USERNAME
        self.base_url = ListenBrainzConfig.BASE_URL
        self.batch_size = ListenBrainzConfig.BATCH_SIZE
        self.throttle = ListenBrainzConfig.THROTTLE
    
    async def fetch_all_listens(self, session: aiohttp.ClientSession, max_ts: int = None) -> List[Dict]:
        """Fetch all listens from ListenBrainz API."""
        url = f"{self.base_url}/user/{self.username}/listens"
        all_listens = []
        latest_ts = max_ts
        
        print(f"📡 Fetching listens for user: {self.username}")
        print(f"   Starting from: {max_ts or 'beginning'}")
        
        while True:
            params = {'count': self.batch_size}
            if latest_ts:
                params['max_ts'] = latest_ts - 1
            
            for attempt in range(5):
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
                        await asyncio.sleep(self.throttle)
                        break
                
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    wait_time = 2 ** attempt
                    print(f"⚠️  Error (attempt {attempt + 1}/5): {e}")
                    await asyncio.sleep(wait_time)
                
                except aiohttp.ClientResponseError as e:
                    if e.status == 429:
                        print("⚠️  Rate limited. Waiting 60s...")
                        await asyncio.sleep(60)
                    else:
                        print(f"❌ HTTP {e.status}: {e}")
                        return all_listens
            else:
                print("❌ Max retries reached")
                break
        
        return all_listens
    
    def save_bronze_parquet(self, listens: List[Dict]) -> str:
        """Save listens to bronze Parquet file."""
        print(f"\n💾 Saving {len(listens)} listens to Parquet...")
        
        bronze_records = []
        for listen in listens:
            bronze_records.append({
                'raw_payload': json.dumps(listen),
                'ingested_at': datetime.now().isoformat(),
                'source_file_name': f'listenbrainz_{self.username}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            })
        
        df = pd.DataFrame(bronze_records)
        output_file = self.output_dir / f'bronze_listens_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        df.to_parquet(output_file, index=False, compression='snappy')
        
        print(f"✅ Saved: {output_file}")
        print(f"   Size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
        return str(output_file)
    
    def extract_mbids(self, listens: List[Dict]) -> Dict[str, List[str]]:
        """Extract unique MBIDs from listens."""
        artist_mbids = set()
        release_mbids = set()
        recording_mbids = set()
        
        for listen in listens:
            metadata = listen.get('track_metadata', {})
            mbid_mapping = metadata.get('mbid_mapping', {})
            additional_info = metadata.get('additional_info', {})
            
            # Artist MBIDs
            artist_list = mbid_mapping.get('artist_mbids', []) or additional_info.get('artist_mbids', [])
            if artist_list:
                artist_mbids.update(artist_list)
            
            # Release MBID
            release_mbid = mbid_mapping.get('release_mbid') or additional_info.get('release_mbid')
            if release_mbid:
                release_mbids.add(release_mbid)
            
            # Recording MBID
            recording_mbid = mbid_mapping.get('recording_mbid') or additional_info.get('recording_mbid')
            if recording_mbid:
                recording_mbids.add(recording_mbid)
        
        print(f"\n📊 Extracted MBIDs:")
        print(f"   Artists: {len(artist_mbids)}")
        print(f"   Releases: {len(release_mbids)}")
        print(f"   Recordings: {len(recording_mbids)}")
        
        return {
            'artist_mbids': list(artist_mbids),
            'release_mbids': list(release_mbids),
            'recording_mbids': list(recording_mbids)
        }
    
    async def run(self, max_ts: int = None):
        """Main export process."""
        print("="*60)
        print("🎵 ListenBrainz → Parquet Exporter")
        print("="*60)
        
        async with aiohttp.ClientSession() as session:
            listens = await self.fetch_all_listens(session, max_ts)
        
        if not listens:
            print("\nℹ️  No listens to export")
            return
        
        # Save bronze
        bronze_file = self.save_bronze_parquet(listens)
        
        # Extract MBIDs
        mbids = self.extract_mbids(listens)
        
        # Save MBIDs for next step
        mbids_file = self.output_dir / f'mbids_to_fetch_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(mbids_file, 'w') as f:
            json.dump(mbids, f, indent=2)
        
        print(f"\n✅ Export complete!")
        print(f"   Bronze: {bronze_file}")
        print(f"   MBIDs: {mbids_file}")


if __name__ == "__main__":
    exporter = ListenBrainzExporter(output_dir="./parquet_export")
    asyncio.run(exporter.run())
