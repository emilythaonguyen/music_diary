# Music Diary Setup Guide

## Environment Setup

### 1. Create your .env file

```bash
# Copy the example file
cp .env.example .env

# Edit .env with your actual credentials
nano .env  # or use any text editor
```

Your `.env` file should look like:

```bash
# ListenBrainz Configuration
LISTENBRAINZ_USERNAME=your_actual_username

# Last.fm API Configuration (optional)
LASTFM_API_KEY=abc123yourapikeyhere

# Output Directory (optional)
OUTPUT_DIR=./parquet_export
```

### 2. Get API Keys

**Last.fm API Key** (optional but recommended):
1. Go to https://www.last.fm/api/account/create
2. Fill in the form:
   - Application name: `Music Diary`
   - Application description: `Personal music listening analysis`
3. Copy the API key (NOT the shared secret)
4. Paste into `.env` file

### 3. Install Dependencies

```bash
# Install Python dependencies
pip install python-dotenv aiohttp pandas pyarrow tqdm
```

## Running the Scripts

### Quick Test (Recommended First)

```bash
# Test with small dataset first
python scripts/test_export.py
```

This will:
- Test with 20 listens (~1 minute)
- Validate API authentication
- Check file creation
- Save test output to `./test_output/`

### Full Export

**Step 1: Export ListenBrainz Data**

```bash
python scripts/export_to_parquet.py
```

Expected output:
- `bronze_listens_YYYYMMDD_HHMMSS.parquet` (~50-80 MB for 115k listens)
- `mbids_to_fetch_YYYYMMDD_HHMMSS.json`

Time: ~10-15 minutes for 115k listens

**Step 2: Fetch Metadata**

```bash
python scripts/fetch_metadata_to_parquet.py
```

Expected output:
- `bronze_musicbrainz_YYYYMMDD_HHMMSS.parquet` (~100-150 MB)
- `bronze_lastfm_YYYYMMDD_HHMMSS.parquet` (if API key provided)
- `bronze_wikidata_YYYYMMDD_HHMMSS.parquet` (~5-10 MB)

Time: ~2-3 hours (due to MusicBrainz rate limiting)

## Security Notes

✅ **Good Practices:**
- `.env` is in `.gitignore` (already configured)
- Never commit API keys to version control
- Use `.env.example` as a template for others

❌ **Don't:**
- Share your `.env` file
- Commit `.env` to git
- Hardcode API keys in scripts

## Troubleshooting

### "ModuleNotFoundError: No module named 'dotenv'"

```bash
pip install python-dotenv
```

### "ListenBrainz username not provided"

Check your `.env` file:
- Make sure it's named `.env` (not `.env.txt`)
- Make sure `LISTENBRAINZ_USERNAME` is set
- Make sure there's no space around the `=`

### "Rate limited by MusicBrainz"

- This is normal, the script respects the 1 req/sec limit
- If you see 503 errors, wait 5 minutes and restart
- Progress is not lost, the script continues where it left off

### "Last.fm API returns errors"

- Check your API key is correct
- Last.fm may not have data for all artists (this is normal)
- The script will skip missing artists and continue

## Next Steps

After export completes:

1. Upload Parquet files to Databricks:
   - Via UI: Volumes → Upload
   - Via CLI: `databricks fs cp bronze_*.parquet dbfs:/Volumes/...`

2. Load into bronze tables (see [README_PARQUET_EXPORT.md](scripts/README_PARQUET_EXPORT.md))

3. Run bronze → silver transformations in Databricks
