# Music Diary: PostgreSQL → Parquet Migration Guide

## Overview

These scripts help you migrate from PostgreSQL ingestion to Parquet files for Databricks. This is essential for Databricks Free Edition, which restricts external API access.

## The Problem

* **Old workflow**: Scripts call APIs directly → insert into PostgreSQL
* **Free Edition limitation**: Outbound internet restricted → API calls may fail
* **Solution**: Run scripts locally → save as Parquet → upload to Databricks

## New Workflow

```
Local Machine                           Databricks
─────────────                          ────────────

1. export_to_parquet.py                4. Load to bronze tables
   ↓ (ListenBrainz API)                   ↓
   bronze_listens.parquet  ────upload───→  bronze_listens_raw
   mbids_to_fetch.json
                                        5. Transform to silver
2. fetch_metadata_to_parquet.py           ↓
   ↓ (MusicBrainz/Wikidata)               silver_listens
   bronze_musicbrainz.parquet ─upload─→   silver_artists
   bronze_wikidata.parquet    ─upload─→   silver_releases
                                           silver_tracks
                                           silver_genre_hierarchy
```

## Prerequisites

```bash
# Install Python dependencies locally
pip install aiohttp pandas pyarrow tqdm
```

## Step 1: Export ListenBrainz Data

**File**: `export_to_parquet.py`

```python
from export_to_parquet import ListenBrainzParquetExporter
import asyncio

# Replace with your username
exporter = ListenBrainzParquetExporter(
    username="your_listenbrainz_username",
    output_dir="./parquet_export"
)

# Run export
asyncio.run(exporter.export())
```

**Output**:
* `bronze_listens_YYYYMMDD_HHMMSS.parquet` → Raw listen JSON payloads
* `mbids_to_fetch_YYYYMMDD_HHMMSS.json` → List of MBIDs to fetch metadata for

**What it does**:
1. Fetches ALL your listens from ListenBrainz API (handles pagination)
2. Stores raw JSON payloads (bronze layer pattern)
3. Extracts unique artist/release/recording MBIDs for next step

## Step 2: Fetch Metadata

**File**: `fetch_metadata_to_parquet.py`

```python
from fetch_metadata_to_parquet import MetadataParquetExporter
import asyncio

# Use the mbids_to_fetch.json from Step 1
exporter = MetadataParquetExporter(
    output_dir="./parquet_export",
    lastfm_api_key=None  # Optional: add Last.fm API key for enrichment
)

asyncio.run(exporter.export("./parquet_export/mbids_to_fetch_20260528_123456.json"))
```

**Output**:
* `bronze_musicbrainz_YYYYMMDD_HHMMSS.parquet` → MusicBrainz artist/release/recording metadata
* `bronze_wikidata_YYYYMMDD_HHMMSS.parquet` → Wikidata genre hierarchy (extracted from MusicBrainz)

**What it does**:
1. Fetches MusicBrainz metadata for all unique MBIDs
2. Respects MusicBrainz rate limit (1 req/sec)
3. Extracts Wikidata IDs from genre tags
4. Fetches Wikidata entities for genre hierarchy traversal

**Note**: This step takes ~2-3 hours for 115k listens due to MusicBrainz rate limiting.

## Step 3: Upload to Databricks

### Option A: Via Databricks UI

1. Navigate to your workspace
2. Create a volume: `CREATE VOLUME IF NOT EXISTS music_diary.raw_data`
3. Click on the volume → Upload files
4. Upload all `bronze_*.parquet` files

### Option B: Via Databricks CLI

```bash
# Upload files
databricks fs cp bronze_listens_*.parquet dbfs:/Volumes/main/music_diary/raw_data/
databricks fs cp bronze_musicbrainz_*.parquet dbfs:/Volumes/main/music_diary/raw_data/
databricks fs cp bronze_wikidata_*.parquet dbfs:/Volumes/main/music_diary/raw_data/
```

## Step 4: Load into Bronze Tables (in Databricks)

```python
# Load listens
df_listens = spark.read.parquet("/Volumes/main/music_diary/raw_data/bronze_listens_*.parquet")
df_listens.write.mode("append").saveAsTable("music_diary.bronze_listens_raw")

# Load MusicBrainz
df_mb = spark.read.parquet("/Volumes/main/music_diary/raw_data/bronze_musicbrainz_*.parquet")
df_mb.write.mode("append").saveAsTable("music_diary.bronze_musicbrainz_raw")

# Load Wikidata
df_wd = spark.read.parquet("/Volumes/main/music_diary/raw_data/bronze_wikidata_*.parquet")
df_wd.write.mode("append").saveAsTable("music_diary.bronze_wikidata_genres")
```

## Step 5: Transform to Silver (in Databricks)

You'll need to create transformation notebooks that:

1. **Parse bronze_listens_raw** → `silver_listens`
   * Extract track_metadata from raw_payload JSON
   * Match to MBIDs from MusicBrainz data
   * Populate artist_mbid, release_mbid, recording_mbid

2. **Parse bronze_musicbrainz_raw** → `silver_artists`, `silver_releases`, `silver_tracks`
   * Extract artist info (name, aliases, genres)
   * Extract release info (name, date, type)
   * Extract track info (name, duration, ISRC)

3. **Parse bronze_wikidata_genres** → `silver_genre_hierarchy`, `silver_genre_mapping`
   * Extract genre labels and parent relationships (P279: subclass of)
   * Build hierarchical genre tree (east coast hip hop → hip hop → music)
   * Map raw MusicBrainz genres to standardized Wikidata names

## Key Differences from PostgreSQL Version

| PostgreSQL Scripts | Parquet Scripts |
|-------------------|----------------|
| Insert records row-by-row | Collect all data, save once |
| get_or_create pattern | No deduplication (handle in Spark) |
| Transactions for consistency | Bronze layer preserves everything |
| Online processing | Offline batch processing |
| ~8 hours runtime | ~3 hours runtime (parallelizable) |

## File Size Estimates (for 115k listens)

* `bronze_listens.parquet`: ~50-80 MB
* `bronze_musicbrainz.parquet`: ~100-150 MB (depends on unique artists/releases)
* `bronze_wikidata.parquet`: ~5-10 MB

**Total**: ~200 MB (vs ~2 GB uncompressed JSON)

## Incremental Updates

To add new listens later:

```python
# Get the latest timestamp from your silver_listens table in Databricks
max_ts = 1735689600  # Example: last listen timestamp

# Run export with max_ts
exporter = ListenBrainzParquetExporter(...)
asyncio.run(exporter.export(max_ts=max_ts))
```

This fetches only NEW listens since the last export.

## Troubleshooting

**"Rate limited by MusicBrainz"**
* The script already respects 1 req/sec
* If you get 503 errors, wait 5 minutes and restart

**"Databricks Free Edition blocks external URLs"**
* That's why we run these scripts LOCALLY, not in Databricks
* Upload the Parquet files after generation

**"File size too large to upload via UI"**
* Use Databricks CLI or split into smaller batches
* Parquet is already compressed (should be <500 MB total)

**"How do I handle duplicates?"**
* Bronze layer preserves everything
* Handle deduplication in silver transformations using `DISTINCT` or window functions

## Next Steps

After loading bronze data:
1. Create bronze → silver transformation notebooks
2. Set up genre hierarchy parsing from Wikidata
3. Build gold layer aggregation tables (top artists, genre trends, etc.)
4. Create dashboards with Databricks SQL

## Questions?

Refer to the bronze/silver table schemas in [00_delta_tables_setup](#notebook-3306402689318542).
