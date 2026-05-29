"""
Fetch MusicBrainz, Last.fm, and Wikidata metadata and save to Parquet.
This script runs LOCALLY after you've exported listens.
"""
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Set

import aiohttp
import pandas as pd
from tqdm import tqdm


class MetadataParquetExporter:
    """Fetch metadata from APIs and save to Parquet files."""
    
    def __init__(self, output_dir: str = "./output", lastfm_api_key: Optional[str] = None):
        """
        Initialize exporter.
        
        Args:
            output_dir: Directory to save Parquet files
            lastfm_api_key: Last.fm API key (optional)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # API configurations
        self.musicbrainz_base = "https://musicbrainz.org/ws/2"
        self.lastfm_base = "https://ws.audioscrobbler.com/2.0/"
        self.wikidata_base = "https://www.wikidata.org/w/api.php"
        
        self.lastfm_api_key = lastfm_api_key
        self.musicbrainz_rate_limit = 1.0  # MusicBrainz requires 1 req/sec
        
        # Storage for collected data
        self.musicbrainz_data = []
        self.lastfm_data = []
        self.wikidata_data = []
    
    async def fetch_musicbrainz_artist(self, session: aiohttp.ClientSession, mbid: str) -> Optional[Dict]:
        """Fetch artist metadata from MusicBrainz."""
        url = f"{self.musicbrainz_base}/artist/{mbid}"
        params = {'fmt': 'json', 'inc': 'genres+tags+aliases+isnis'}
        
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                
                return {
                    'mbid': mbid,
                    'query_term': data.get('name', ''),
                    'entity_type': 'artist',
                    'raw_payload': json.dumps(data),
                    'ingested_at': datetime.now().isoformat()
                }
        except Exception as e:
            print(f"Error fetching artist {mbid}: {e}")
            return None
    
    async def fetch_musicbrainz_release(self, session: aiohttp.ClientSession, mbid: str) -> Optional[Dict]:
        """Fetch release metadata from MusicBrainz."""
        url = f"{self.musicbrainz_base}/release/{mbid}"
        params = {'fmt': 'json', 'inc': 'artists+recordings+genres+tags'}
        
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                
                return {
                    'mbid': mbid,
                    'query_term': data.get('title', ''),
                    'entity_type': 'release',
                    'raw_payload': json.dumps(data),
                    'ingested_at': datetime.now().isoformat()
                }
        except Exception as e:
            print(f"Error fetching release {mbid}: {e}")
            return None
    
    async def fetch_musicbrainz_recording(self, session: aiohttp.ClientSession, mbid: str) -> Optional[Dict]:
        """Fetch recording metadata from MusicBrainz."""
        url = f"{self.musicbrainz_base}/recording/{mbid}"
        params = {'fmt': 'json', 'inc': 'artists+releases+genres+tags+isrcs'}
        
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                
                return {
                    'mbid': mbid,
                    'query_term': data.get('title', ''),
                    'entity_type': 'recording',
                    'raw_payload': json.dumps(data),
                    'ingested_at': datetime.now().isoformat()
                }
        except Exception as e:
            print(f"Error fetching recording {mbid}: {e}")
            return None
    
    async def fetch_lastfm_artist(self, session: aiohttp.ClientSession, artist_name: str) -> Optional[Dict]:
        """Fetch artist info from Last.fm."""
        if not self.lastfm_api_key:
            return None
        
        params = {
            'method': 'artist.getinfo',
            'artist': artist_name,
            'api_key': self.lastfm_api_key,
            'format': 'json'
        }
        
        try:
            async with session.get(self.lastfm_base, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                
                return {
                    'entity_id': artist_name,
                    'entity_type': 'artist',
                    'raw_payload': json.dumps(data),
                    'ingested_at': datetime.now().isoformat()
                }
        except Exception as e:
            print(f"Error fetching Last.fm artist {artist_name}: {e}")
            return None
    
    async def fetch_wikidata_entity(self, session: aiohttp.ClientSession, wikidata_id: str, source_genre: str) -> Optional[Dict]:
        """Fetch Wikidata entity for genre hierarchy."""
        params = {
            'action': 'wbgetentities',
            'ids': wikidata_id,
            'format': 'json',
            'props': 'labels|claims'
        }
        
        try:
            async with session.get(self.wikidata_base, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                
                return {
                    'wikidata_id': wikidata_id,
                    'musicbrainz_genre': source_genre,
                    'raw_payload': json.dumps(data),
                    'ingested_at': datetime.now().isoformat()
                }
        except Exception as e:
            print(f"Error fetching Wikidata {wikidata_id}: {e}")
            return None
    
    async def fetch_musicbrainz_metadata(self, mbids: Dict[str, List[str]]):
        """Fetch all MusicBrainz metadata."""
        print("\n📡 Fetching MusicBrainz metadata...")
        
        async with aiohttp.ClientSession(headers={'User-Agent': 'MusicDiary/1.0'}) as session:
            # Fetch artists
            print(f"   Artists: {len(mbids.get('artist_mbids', []))}")
            for mbid in tqdm(mbids.get('artist_mbids', []), desc="Artists"):
                data = await self.fetch_musicbrainz_artist(session, mbid)
                if data:
                    self.musicbrainz_data.append(data)
                await asyncio.sleep(self.musicbrainz_rate_limit)
            
            # Fetch releases
            print(f"   Releases: {len(mbids.get('release_mbids', []))}")
            for mbid in tqdm(mbids.get('release_mbids', []), desc="Releases"):
                data = await self.fetch_musicbrainz_release(session, mbid)
                if data:
                    self.musicbrainz_data.append(data)
                await asyncio.sleep(self.musicbrainz_rate_limit)
            
            # Fetch recordings
            print(f"   Recordings: {len(mbids.get('recording_mbids', []))}")
            for mbid in tqdm(mbids.get('recording_mbids', []), desc="Recordings"):
                data = await self.fetch_musicbrainz_recording(session, mbid)
                if data:
                    self.musicbrainz_data.append(data)
                await asyncio.sleep(self.musicbrainz_rate_limit)
    
    def extract_wikidata_ids_from_musicbrainz(self) -> Set[tuple]:
        """Extract Wikidata IDs from MusicBrainz genre tags."""
        wikidata_mappings = set()
        
        for record in self.musicbrainz_data:
            payload = json.loads(record['raw_payload'])
            genres = payload.get('genres', [])
            
            for genre in genres:
                # MusicBrainz sometimes includes Wikidata IDs in genre metadata
                wikidata_id = genre.get('wikidata-id')
                if wikidata_id:
                    wikidata_mappings.add((wikidata_id, genre.get('name', '')))
        
        return wikidata_mappings
    
    async def fetch_wikidata_metadata(self, wikidata_mappings: Set[tuple]):
        """Fetch Wikidata genre hierarchy."""
        if not wikidata_mappings:
            print("\n⚠️ No Wikidata IDs found in MusicBrainz data")
            return
        
        print(f"\n📡 Fetching Wikidata metadata for {len(wikidata_mappings)} genres...")
        
        async with aiohttp.ClientSession() as session:
            for wikidata_id, genre_name in tqdm(list(wikidata_mappings), desc="Wikidata"):
                data = await self.fetch_wikidata_entity(session, wikidata_id, genre_name)
                if data:
                    self.wikidata_data.append(data)
                await asyncio.sleep(0.1)  # Be nice to Wikidata
    
    def save_to_parquet(self):
        """Save all collected data to Parquet files."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save MusicBrainz data
        if self.musicbrainz_data:
            df = pd.DataFrame(self.musicbrainz_data)
            output_file = self.output_dir / f'bronze_musicbrainz_{timestamp}.parquet'
            df.to_parquet(output_file, index=False, compression='snappy')
            print(f"✅ Saved MusicBrainz: {output_file} ({len(self.musicbrainz_data)} records)")
        
        # Save Last.fm data
        if self.lastfm_data:
            df = pd.DataFrame(self.lastfm_data)
            output_file = self.output_dir / f'bronze_lastfm_{timestamp}.parquet'
            df.to_parquet(output_file, index=False, compression='snappy')
            print(f"✅ Saved Last.fm: {output_file} ({len(self.lastfm_data)} records)")
        
        # Save Wikidata data
        if self.wikidata_data:
            df = pd.DataFrame(self.wikidata_data)
            output_file = self.output_dir / f'bronze_wikidata_{timestamp}.parquet'
            df.to_parquet(output_file, index=False, compression='snappy')
            print(f"✅ Saved Wikidata: {output_file} ({len(self.wikidata_data)} records)")
    
    async def export(self, mbids_file: str):
        """Main export process."""
        print("="*60)
        print("🎵 Metadata → Parquet Exporter")
        print("="*60)
        
        # Load MBIDs from previous step
        with open(mbids_file, 'r') as f:
            mbids = json.load(f)
        
        # Fetch MusicBrainz metadata
        await self.fetch_musicbrainz_metadata(mbids)
        
        # Extract Wikidata IDs from MusicBrainz genres
        wikidata_mappings = self.extract_wikidata_ids_from_musicbrainz()
        
        # Fetch Wikidata metadata
        await self.fetch_wikidata_metadata(wikidata_mappings)
        
        # Save everything
        print("\n💾 Saving to Parquet files...")
        self.save_to_parquet()
        
        print("\n✅ Export complete!")
        print("\n📦 Next steps:")
        print("   1. Upload all bronze_*.parquet files to Databricks")
        print("   2. Load into bronze tables using Spark")
        print("   3. Run bronze → silver transformations")


def main():
    """
    Example usage.
    """
    # Paths from previous export
    MBIDS_FILE = "./parquet_export/mbids_to_fetch_20260528_123456.json"
    OUTPUT_DIR = "./parquet_export"
    
    # Optional: Last.fm API key for enrichment
    LASTFM_API_KEY = None  # Set this if you want Last.fm data
    
    # Create exporter
    exporter = MetadataParquetExporter(
        output_dir=OUTPUT_DIR,
        lastfm_api_key=LASTFM_API_KEY
    )
    
    # Run export
    asyncio.run(exporter.export(MBIDS_FILE))


if __name__ == "__main__":
    main()
