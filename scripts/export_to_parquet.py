"""
Export ListenBrainz data to Parquet files for Databricks ingestion.
This script runs LOCALLY and saves bronze-layer Parquet files.
"""
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

import aiohttp
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class ListenBrainzParquetExporter:
    """Export ListenBrainz data to Parquet files."""
    
    def __init__(self, username: str = None, output_dir: str = "./output"):
        """
        Initialize exporter.
        
        Args:
            username: ListenBrainz username (if not provided, loads from .env)
            output_dir: Directory to save Parquet files
        """
        self.username = username or os.getenv('LISTENBRAINZ_USERNAME')
        if not self.username:
            raise ValueError(
                "ListenBrainz username not provided. "
                "Either pass username parameter or set LISTENBRAINZ_USERNAME in .env file"
            )
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.base_url = "https://api.listenbrainz.org/1"
        self.batch_size = 1000
        self.throttle = 1.0  # seconds between requests
    
    async def fetch_all_listens(
        self,
        session: aiohttp.ClientSession,
        max_ts: Optional[int] = None
    ) -> List[Dict]:
        """
        Fetch all listens from ListenBrainz API.
        
        Args:
            session: aiohttp session
            max_ts: Maximum timestamp to fetch (for incremental updates)
        
        Returns:
            List of listen objects
        """
        url = f"{self.base_url}/user/{self.username}/listens"
        all_listens = []
        latest_ts = max_ts
        
        print(f"📡 Fetching listens for user: {self.username}")
        print(f"   Starting from: {max_ts or 'beginning'}")
        
        while True:
            params = {'count': self.batch_size}
            if latest_ts:
                params['max_ts'] = latest_ts - 1
            
            # Retry logic for API calls
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
                        
                        print(f"   Batch: {len(batch)} listens | Total: {len(all_listens)}")
                        await asyncio.sleep(self.throttle)
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
    
    def save_bronze_listens(self, listens: List[Dict]) -> str:
        """
        Save listens to bronze layer Parquet file.
        
        Args:
            listens: List of listen objects from API
        
        Returns:
            Path to saved file
        """
        print(f"\n💾 Saving {len(listens)} listens to bronze layer...")
        
        # Create bronze records
        bronze_records = []
        for listen in listens:
            bronze_records.append({
                'raw_payload': json.dumps(listen),
                'ingested_at': datetime.now().isoformat(),
                'source_file_name': f'listenbrainz_{self.username}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            })
        
        # Save to Parquet
        df = pd.DataFrame(bronze_records)
        output_file = self.output_dir / f'bronze_listens_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        df.to_parquet(output_file, index=False, compression='snappy')
        
        print(f"✅ Saved to: {output_file}")
        print(f"   File size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
        return str(output_file)
    
    def extract_metadata_for_api_calls(self, listens: List[Dict]) -> Dict[str, List[str]]:
        """
        Extract unique MBIDs from listens for subsequent API calls.
        
        Args:
            listens: List of listen objects
        
        Returns:
            Dict with sets of artist_mbids, release_mbids, recording_mbids
        """
        artist_mbids = set()
        release_mbids = set()
        recording_mbids = set()
        
        for listen in listens:
            metadata = listen.get('track_metadata', {})
            mbid_mapping = metadata.get('mbid_mapping', {})
            additional_info = metadata.get('additional_info', {})
            
            # Collect MBIDs
            artist_list = mbid_mapping.get('artist_mbids', []) or additional_info.get('artist_mbids', [])
            if artist_list:
                artist_mbids.update(artist_list)
            
            release_mbid = mbid_mapping.get('release_mbid') or additional_info.get('release_mbid')
            if release_mbid:
                release_mbids.add(release_mbid)
            
            recording_mbid = mbid_mapping.get('recording_mbid') or additional_info.get('recording_mbid')
            if recording_mbid:
                recording_mbids.add(recording_mbid)
        
        print(f"\n📊 Extracted metadata identifiers:")
        print(f"   Unique artists: {len(artist_mbids)}")
        print(f"   Unique releases: {len(release_mbids)}")
        print(f"   Unique recordings: {len(recording_mbids)}")
        
        return {
            'artist_mbids': list(artist_mbids),
            'release_mbids': list(release_mbids),
            'recording_mbids': list(recording_mbids)
        }
    
    async def export(self, max_ts: Optional[int] = None):
        """
        Main export process.
        
        Args:
            max_ts: Maximum timestamp for incremental export
        """
        print("="*60)
        print("🎵 ListenBrainz → Parquet Exporter")
        print("="*60)
        
        # Fetch listens
        async with aiohttp.ClientSession() as session:
            listens = await self.fetch_all_listens(session, max_ts)
        
        if not listens:
            print("\nℹ️ No listens to export")
            return
        
        # Save bronze layer
        bronze_file = self.save_bronze_listens(listens)
        
        # Extract metadata for future API calls
        mbids = self.extract_metadata_for_api_calls(listens)
        
        # Save MBID lists for next steps
        mbids_file = self.output_dir / f'mbids_to_fetch_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(mbids_file, 'w') as f:
            json.dump(mbids, f, indent=2)
        
        print(f"\n✅ Export complete!")
        print(f"   Bronze file: {bronze_file}")
        print(f"   MBIDs list: {mbids_file}")
        print(f"\n📦 Next steps:")
        print(f"   1. Upload {Path(bronze_file).name} to Databricks (via Volumes or workspace)")
        print(f"   2. Run: python fetch_metadata_to_parquet.py")
        print(f"   3. Run bronze → silver transformation in Databricks")


def main():
    """
    Example usage.
    """
    # Username and output directory loaded from .env or defaults
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', './parquet_export')
    
    # Create exporter (username loaded from .env)
    exporter = ListenBrainzParquetExporter(output_dir=OUTPUT_DIR)
    
    # Run export
    asyncio.run(exporter.export())


if __name__ == "__main__":
    main()
