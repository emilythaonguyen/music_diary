"""
Test the export scripts with a small dataset before running full export.
Run this LOCALLY (not in Databricks) to validate your setup.
"""
import asyncio
import json
import sys
from pathlib import Path

import pandas as pd

# Add parent directory to path to import your modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.export_to_parquet import ListenBrainzParquetExporter
from scripts.fetch_metadata_to_parquet import MetadataParquetExporter


def validate_parquet_file(file_path: Path, expected_columns: list) -> bool:
    """Validate that a Parquet file exists and has expected structure."""
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return False
    
    try:
        df = pd.read_parquet(file_path)
        print(f"✅ {file_path.name}")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        print(f"   Size: {file_path.stat().st_size / 1024:.2f} KB")
        
        # Check for expected columns
        missing_cols = set(expected_columns) - set(df.columns)
        if missing_cols:
            print(f"⚠️  Missing columns: {missing_cols}")
            return False
        
        # Check for empty data
        if len(df) == 0:
            print("⚠️  File is empty!")
            return False
        
        # Sample first row
        print(f"   Sample row:")
        for col in df.columns:
            value = str(df[col].iloc[0])[:50]  # Truncate long values
            print(f"     {col}: {value}...")
        
        return True
    
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
        return False


async def test_listenbrainz_export(username: str, test_size: int = 20):
    """Test ListenBrainz export with small dataset."""
    print("="*60)
    print("TEST 1: ListenBrainz Export")
    print("="*60)
    
    output_dir = Path("./test_output")
    output_dir.mkdir(exist_ok=True)
    
    # Create exporter
    exporter = ListenBrainzParquetExporter(
        username=username,
        output_dir=str(output_dir)
    )
    
    # Monkey-patch to limit results for testing
    original_fetch = exporter.fetch_all_listens
    
    async def limited_fetch(session, max_ts=None):
        listens = await original_fetch(session, max_ts)
        if len(listens) > test_size:
            print(f"⚠️  Limiting to {test_size} listens for testing")
            return listens[:test_size]
        return listens
    
    exporter.fetch_all_listens = limited_fetch
    
    # Run export
    try:
        await exporter.export()
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return False
    
    # Validate outputs
    print("\n" + "="*60)
    print("Validating ListenBrainz outputs...")
    print("="*60)
    
    # Find the generated files
    bronze_files = list(output_dir.glob("bronze_listens_*.parquet"))
    mbids_files = list(output_dir.glob("mbids_to_fetch_*.json"))
    
    if not bronze_files:
        print("❌ No bronze_listens file created")
        return False
    
    if not mbids_files:
        print("❌ No mbids_to_fetch file created")
        return False
    
    # Validate Parquet file
    success = validate_parquet_file(
        bronze_files[0],
        expected_columns=['raw_payload', 'ingested_at', 'source_file_name']
    )
    
    # Validate MBIDs JSON
    print(f"\n✅ {mbids_files[0].name}")
    with open(mbids_files[0], 'r') as f:
        mbids = json.load(f)
        print(f"   Artist MBIDs: {len(mbids.get('artist_mbids', []))}")
        print(f"   Release MBIDs: {len(mbids.get('release_mbids', []))}")
        print(f"   Recording MBIDs: {len(mbids.get('recording_mbids', []))}")
    
    return success


async def test_metadata_export(mbids_file: Path, lastfm_api_key: str = None, test_size: int = 5):
    """Test metadata export with small dataset."""
    print("\n" + "="*60)
    print("TEST 2: Metadata Export")
    print("="*60)
    
    if not mbids_file.exists():
        print(f"❌ MBIDs file not found: {mbids_file}")
        print("   Run Test 1 first to generate mbids_to_fetch.json")
        return False
    
    output_dir = mbids_file.parent
    
    # Load and limit MBIDs for testing
    with open(mbids_file, 'r') as f:
        mbids = json.load(f)
    
    # Limit to test_size for each category
    limited_mbids = {
        'artist_mbids': mbids.get('artist_mbids', [])[:test_size],
        'release_mbids': mbids.get('release_mbids', [])[:test_size],
        'recording_mbids': mbids.get('recording_mbids', [])[:test_size]
    }
    
    # Save limited version
    test_mbids_file = output_dir / "mbids_test.json"
    with open(test_mbids_file, 'w') as f:
        json.dump(limited_mbids, f, indent=2)
    
    print(f"Testing with {test_size} MBIDs per category...")
    
    # Create exporter
    exporter = MetadataParquetExporter(
        output_dir=str(output_dir),
        lastfm_api_key=lastfm_api_key
    )
    
    # Run export
    try:
        await exporter.export(str(test_mbids_file))
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return False
    
    # Validate outputs
    print("\n" + "="*60)
    print("Validating metadata outputs...")
    print("="*60)
    
    success = True
    
    # Check MusicBrainz
    mb_files = list(output_dir.glob("bronze_musicbrainz_*.parquet"))
    if mb_files:
        success &= validate_parquet_file(
            mb_files[-1],  # Most recent
            expected_columns=['mbid', 'query_term', 'entity_type', 'raw_payload', 'ingested_at']
        )
    else:
        print("❌ No bronze_musicbrainz file created")
        success = False
    
    # Check Last.fm (optional)
    lastfm_files = list(output_dir.glob("bronze_lastfm_*.parquet"))
    if lastfm_files:
        print("\n")
        success &= validate_parquet_file(
            lastfm_files[-1],
            expected_columns=['entity_id', 'entity_type', 'raw_payload', 'ingested_at']
        )
    elif lastfm_api_key:
        print("⚠️  No Last.fm data (API key may be invalid or artists not found)")
    else:
        print("ℹ️  Last.fm skipped (no API key provided)")
    
    # Check Wikidata
    wd_files = list(output_dir.glob("bronze_wikidata_*.parquet"))
    if wd_files:
        print("\n")
        success &= validate_parquet_file(
            wd_files[-1],
            expected_columns=['wikidata_id', 'musicbrainz_genre', 'raw_payload', 'ingested_at']
        )
    else:
        print("⚠️  No Wikidata data (MusicBrainz may not have genre Wikidata IDs)")
    
    return success


async def main():
    """
    Run all tests.
    """
    print("🧪 Music Diary Export Tests")
    print("="*60)
    print("This will test your export scripts with a small dataset.")
    print("Make sure you're running this LOCALLY (not in Databricks).")
    print("="*60)
    
    # Configuration
    USERNAME = input("\nEnter your ListenBrainz username: ").strip()
    if not USERNAME:
        print("❌ Username required")
        return
    
    LASTFM_API_KEY = input("Enter Last.fm API key (optional, press Enter to skip): ").strip()
    if not LASTFM_API_KEY:
        LASTFM_API_KEY = None
    
    # Test parameters
    LISTEN_TEST_SIZE = 20  # Test with 20 listens
    METADATA_TEST_SIZE = 5  # Test with 5 MBIDs per category
    
    print(f"\nTest configuration:")
    print(f"  ListenBrainz user: {USERNAME}")
    print(f"  Last.fm API: {'✅ Provided' if LASTFM_API_KEY else '❌ Not provided'}")
    print(f"  Listen test size: {LISTEN_TEST_SIZE}")
    print(f"  Metadata test size: {METADATA_TEST_SIZE} per category")
    
    input("\nPress Enter to start tests...")
    
    # Run Test 1: ListenBrainz export
    test1_passed = await test_listenbrainz_export(USERNAME, LISTEN_TEST_SIZE)
    
    if not test1_passed:
        print("\n❌ Test 1 failed. Fix issues before proceeding.")
        return
    
    # Find the mbids file from Test 1
    test_output = Path("./test_output")
    mbids_files = list(test_output.glob("mbids_to_fetch_*.json"))
    
    if not mbids_files:
        print("\n❌ Cannot proceed to Test 2: No MBIDs file found")
        return
    
    # Run Test 2: Metadata export
    test2_passed = await test_metadata_export(
        mbids_files[0],
        LASTFM_API_KEY,
        METADATA_TEST_SIZE
    )
    
    # Final summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Test 1 (ListenBrainz): {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"Test 2 (Metadata): {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 All tests passed!")
        print("\nYou're ready to run the full export:")
        print("  1. python export_to_parquet.py")
        print("  2. python fetch_metadata_to_parquet.py")
    else:
        print("\n⚠️  Some tests failed. Review the errors above.")
    
    print(f"\nTest files saved to: {test_output.absolute()}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
