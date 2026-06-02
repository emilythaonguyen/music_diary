"""
Simplified export script that reuses your existing MusicBrainz and Last.fm clients.
Run this LOCALLY to export data to Parquet files for Databricks.
"""
import os
import asyncio
import json
import sys
from datetime import datetime
from typing import List, Dict, Set, Optional, Tuple

import aiohttp
import pandas as pd
from tqdm import tqdm

# import existing client service classes
from services.musicbrainz import MusicBrainzClient
from services.lastfm import LastFMClient

# import existing configuration parameters
from core.config import ListenBrainzConfig, MusicBrainzConfig, LastFMConfig


class DataExporter:
    """Handles local fat-extraction of music diary components into Bronze Parquet files."""
    
    def __init__(self, output_dir: str = "./parquet_export"):
        self.output_dir = output_dir
        self.mb_client = MusicBrainzClient()
        self.lastfm_client = LastFMClient()
        
        # storage
        self.musicbrainz_data: List[Dict] = []
        self.lastfm_data: List[Dict] = []
        self.wikidata_data: List[Dict] = []

        # create directory
        os.makedirs(self.output_dir, exist_ok=True)
        
    async def fetch_listenbrainz_data(
        self, 
        max_ts: Optional[int] = None,
        min_ts: Optional[int] = None,
        sample_only: bool = False
    ) -> List[Dict]:
        """Fetch ListenBrainz listens."""
        url = f"{ListenBrainzConfig.BASE_URL}/user/{ListenBrainzConfig.USERNAME}/listens"
        all_listens = []
        latest_ts = max_ts
        
        print("\n📡 Fetching ListenBrainz data...")
        if min_ts:
            print(f"   ⏳ Bounded Window Mode Active (Min Timestamp: {min_ts})")
        
        async with aiohttp.ClientSession() as session:
            while True:
                count = 50 if sample_only else ListenBrainzConfig.BATCH_SIZE
                params = {'count': count}
                if latest_ts:
                    params['max_ts'] = latest_ts - 1
                
                try:
                    async with session.get(url, params=params, timeout=15) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                        batch = data.get('payload', {}).get('listens', [])
                        
                        if not batch:
                            break
                        
                        all_listens.extend(batch)
                        print(f"   Batch: {len(batch)} | Total: {len(all_listens)}")

                        if sample_only:
                            print(f"✅ Smoke test sample caught {len(all_listens)} listens.")
                            break
                        
                        latest_ts = min(l['listened_at'] for l in batch)
                        await asyncio.sleep(ListenBrainzConfig.THROTTLE)
                        
                except Exception as e:
                    print(f"❌ Error fetching listens: {e}")
                    break
                
        print(f"✅ Download complete. Total items loaded: {len(all_listens)}")
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
            mbid_mapping = metadata.get('mbid_mapping') or {}
            additional_info = metadata.get('additional_info') or {}
            
            # Artists
            artists = mbid_mapping.get('artist_mbids') or additional_info.get('artist_mbids') or []
            release = mbid_mapping.get('release_mbid') or additional_info.get('release_mbid')
            recording = mbid_mapping.get('recording_mbid') or additional_info.get('recording_mbid')
            
            if isinstance(artists, list):
                for ambid in artists:
                    if ambid: mbids['artist_mbids'].add(ambid)
            elif isinstance(artists, str) and artists:
                mbids['artist_mbids'].add(artists)
            
            if release: mbids['release_mbids'].add(release)
            if recording: mbids['recording_mbids'].add(recording)
            
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
            try:
                data = await self.mb_client.get_artist(
                    mbid,
                    includes=['tags', 'aliases', 'url-rels']
                )
                if data:
                    self.musicbrainz_data.append({
                        'mbid': mbid,
                        'query_term': data.get('name', 'Unknown Artist'),
                        'entity_type': 'artist',
                        'raw_payload': json.dumps(data),
                        'ingested_at': datetime.now().isoformat()
                    })
            except Exception as e:
                print(f"⚠️ Skipping Artist MBID {mbid} due to exception: {e}")
            await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
        
        # Fetch releases
        release_group_mbids = set()
        
        for mbid in tqdm(list(mbids['release_mbids']), desc="Releases"):
            try:
                data = await self.mb_client.get_release(
                    mbid,
                    includes=['artists', 'recordings', 'tags', 'release-groups']
                )
                if data:
                    self.musicbrainz_data.append({
                        'mbid': mbid,
                        'query_term': data.get('title', 'Unknoen Release'),
                        'entity_type': 'release',
                        'raw_payload': json.dumps(data),
                        'ingested_at': datetime.now().isoformat()
                    })
                    # fetch release group id
                    rg_info = data.get('release-group', {})
                    rg_id = rg_info.get('id')
                    if rg_id:
                        release_group_mbids.add(rg_id)
                        
            except Exception as e:
                print(f"⚠️ Skipping Release MBID {mbid} due to exception: {e}")
            await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
        
        # fetch album-level tags & genres explicitly
        if release_group_mbids:
            for mbid in tqdm(list(release_group_mbids), desc='Release Groups Metadata'):
                try:
                    data = await self.mb_client.get_release_group(
                        mbid,
                        includes=['artists', 'tags']
                    )
                    if data:
                        self.musicbrainz_data.append({
                            'mbid': mbid,
                            'query_term': data.get('title', 'Unknown Release Group'),
                            'entity_type': 'release_group',
                            'raw_payload': json.dumps(data),
                            'ingested_at': datetime.now().isoformat()
                        })
                except Exception as e:
                    print(f"⚠️ Skipping Release Group MBID {mbid} due to exception: {e}")
                await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
        
        # Fetch recordings
        for mbid in tqdm(list(mbids['recording_mbids']), desc="Recordings"):
            try:
                data = await self.mb_client.get_recording(
                    mbid,
                    includes=['artists', 'releases', 'tags', 'isrcs']
                )
                if data:
                    self.musicbrainz_data.append({
                        'mbid': mbid,
                        'query_term': data.get('title', 'Unknown Recording'),
                        'entity_type': 'recording',
                        'raw_payload': json.dumps(data),
                        'ingested_at': datetime.now().isoformat()
                    })
            except Exception as e:
                print(f"⚠️ Skipping Recording MBID {mbid} due to exception: {e}")
            await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
    
    def extract_wikidata_ids(self) -> Set[Tuple[str, str]]:
        """Extract Wikidata IDs from Artist URL relations in MusicBrainz data."""
        wikidata_mappings = set()

        for record in self.musicbrainz_data:
            if record['entity_type'] == 'artist':
                payload = json.loads(record['raw_payload'])

                # Parse the URL relations for the wikidata type
                for rel in payload.get('url-relation-list', []):
                    if rel.get('type') == 'wikidata':
                        target_url = rel.get('target', '')
                        if isinstance(target_url, dict):
                            target_url = target_url.get('id', '')
                        
                        if target_url:
                            # Extract the Q-ID (e.g., https://www.wikidata.org/wiki/Q11649 -> Q11649)
                            wikidata_id = target_url.split('/')[-1].split('?')[0]
                            artist_name = payload.get('name', 'Unknown Artist')
                            if wikidata_id.startswith('Q'):
                                wikidata_mappings.add((wikidata_id, artist_name))

        return wikidata_mappings    
    
    async def fetch_wikidata_metadata(self, wikidata_mappings: Set[Tuple[str, str]]):
        """Fetch Wikidata metadata."""
        if not wikidata_mappings:
            print("\n⚠️  No Wikidata IDs found")
            return
        
        print(f"\n📡 Fetching Wikidata for {len(wikidata_mappings)} genres...")
        url = "https://www.wikidata.org/w/api.php"
        
        async with aiohttp.ClientSession() as session:
            for qid, artist_name in tqdm(wikidata_mappings, desc="Wikidata Lookup"):
                params = {
                    'action': 'wbgetentities',
                    'ids': qid,
                    'format': 'json',
                    'languages': 'en'
                }
                headers = {'User-Agent': MusicBrainzConfig.USER_AGENT}
                
                try:
                    async with session.get(url, params=params, headers=headers, timeout=10) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                        
                        # Ensure the 'entities' key exists and the requested QID is present
                        if 'entities' in data and qid in data['entities']:
                            self.wikidata_data.append({
                                'wikidata_id': qid,
                                'query_term': artist_name,
                                'raw_payload': json.dumps(data['entities'][qid]),
                                'ingested_at': datetime.now().isoformat()
                            })
                        
                except Exception as e:
                    print(f"Error fetching {qid}: {e}")
                await asyncio.sleep(0.1)
    
    async def fetch_lastfm_metadata(self, listens: List[Dict]):
        """Query Last.fm for metrics, passing the required session and text parameters positionally."""
        print("\n📡 Fetching Last.fm metadata (using existing client)...")
        queried_entities = set()
        
        # Deduplicate entities before hitting network loop
        for listen in listens:
            meta = listen.get('track_metadata', {})
            mbid_map = meta.get('mbid_mapping') or {}
            add_info = meta.get('additional_info') or {}
            
            artist_mbid = mbid_map.get('artist_mbids') or add_info.get('artist_mbids')
            if isinstance(artist_mbid, list) and artist_mbid:
                artist_mbid = artist_mbid[0]
                
            track_mbid = mbid_map.get('recording_mbid') or add_info.get('recording_mbid')
            artist_name = meta.get('artist_name')
            track_name = meta.get('track_name')
            
            if artist_name and track_name:
                queried_entities.add((artist_mbid, track_mbid, artist_name, track_name))

        async with aiohttp.ClientSession() as session:
            for ambid, tmbid, artist, track in tqdm(list(queried_entities), desc="Last.fm Sync"):
                try:
                    # 🎯 FIX 2: Pass session, artist, and track positionally. Supply mbid if available.
                    data = await self.lastfm_client.get_track_info(session, artist, track, mbid=tmbid)
                        
                    if data:
                        self.lastfm_data.append({
                            'artist_mbid': ambid or '',
                            'track_mbid': tmbid or '',
                            'artist_name': artist,
                            'track_name': track,
                            'raw_payload': json.dumps(data),
                            'ingested_at': datetime.now().isoformat()
                        })
                except Exception as e:
                    print(f"⚠️ Last.fm request dropped for {artist} - {track}. Reason: {e}")
                await asyncio.sleep(0.3)
            
    def save_to_parquet(self, listens: List[Dict]):
        """Save all data to Parquet files."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print("\n💾 Saving to Parquet files...")
        listens_flat = [{
            'listen_id': f"{l.get('listened_at')}_{l.get('user_name')}",
            'raw_payload': json.dumps(l),
            'ingested_at': datetime.now().isoformat()} for l in listens
        ]
        pd.DataFrame(listens_flat).to_parquet(f"{self.output_dir}/bronze_listens_{timestamp}.parquet", index=False)
        
        # 2. Save MusicBrainz data
        if self.musicbrainz_data:
            pd.DataFrame(self.musicbrainz_data).to_parquet(f"{self.output_dir}/bronze_musicbrainz_{timestamp}.parquet", index=False)
            
        # 3. Save Wikidata data
        if self.wikidata_data:
            pd.DataFrame(self.wikidata_data).to_parquet(f"{self.output_dir}/bronze_wikidata_{timestamp}.parquet", index=False)
            
        # 4. Save Last.fm data
        if self.lastfm_data:
            pd.DataFrame(self.lastfm_data).to_parquet(f"{self.output_dir}/bronze_lastfm_{timestamp}.parquet", index=False)
            
        print(f"✨ Success! Staging snapshots compiled safely under: '{self.output_dir}'")
    
    async def export_all(self, max_ts: Optional[int] = None, min_ts: Optional[int] = None, sample_only: bool = False):
        """Run full export pipeline."""
        print("="*60)
        print("🎵 Music Diary → Parquet Exporter")
        print("="*60)
        
        # Step 1: Fetch ListenBrainz data
        listens = await self.fetch_listenbrainz_data(max_ts, min_ts=min_ts, sample_only=sample_only)
        if not listens:
            print("❌ No listens to export")
            return
        
        # Step 2: Extract MBIDs
        mbids = self.extract_mbids(listens)
        
        # Step 3: Fetch MusicBrainz metadata
        await self.fetch_musicbrainz_metadata(mbids)
        
        # Step 4: Extract and fetch Wikidata (moved to run immediately after MusicBrainz)
        wikidata_mappings = self.extract_wikidata_ids()
        await self.fetch_wikidata_metadata(wikidata_mappings)
        
        # Step 5: Fetch Last.fm metadata (moved after Wikidata)
        await self.fetch_lastfm_metadata(listens)
        
        # Step 6: Save everything
        self.save_to_parquet(listens)
        
        print("\n✅ Export complete!")
        print(f"   Output directory: {os.path.abspath(self.output_dir)}")


async def main():
    """Run the exporter."""
    import os
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', './parquet_export')
    
    sample_only = "--sample" in sys.argv or "--smoke" in sys.argv
    
    exporter = DataExporter(output_dir=OUTPUT_DIR)
    
    await exporter.export_all(sample_only=sample_only)
    
if __name__ == "__main__":
    # if run directly as a standalone script, orchestrate the loop here
    asyncio.run(main())