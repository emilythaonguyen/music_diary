import os
import asyncio
import asyncpg
import aiohttp
import csv
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Database & File Config
# -----------------------------
DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}
LOG_FILE = "release_mismatches.csv"

# -----------------------------
# MusicBrainz Setup
# -----------------------------
MB_BASE = "https://musicbrainz.org/ws/2"
HEADERS = {
    "User-Agent": os.getenv("MB_USER_AGENT") or "music-diary/1.0 (contact@example.com)"
}

rg_cache = {}
async def fetch_release_group(session, release_mbid):
    url = f"{MB_BASE}/release/{release_mbid}"
    params = {"inc": "release-groups", "fmt": "json"}

    try:
        async with session.get(
            url,
            params=params,
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:

            if resp.status == 404:
                return None

            if resp.status == 503:
                await asyncio.sleep(5)
                return await fetch_release_group(session, release_mbid)

            if resp.status != 200:
                return None

            data = await resp.json()
            rg = data.get("release-group")
            return rg.get("id") if rg else None

    except (asyncio.TimeoutError, aiohttp.ClientError):
        return None

async def main():
    async with asyncpg.create_pool(**DB_CONFIG) as pool:
        
        releases = await pool.fetch("""
            SELECT release_id, release_name, release_mbid, release_group_mbid
            FROM releases
            WHERE release_mbid IS NOT NULL
              AND release_group_mbid IS NOT NULL;
        """)

        total = len(releases)
        print(f"🧐 Processing {total} releases. Logging mismatches to {LOG_FILE}...")

        # Open CSV file and write header
        with open(LOG_FILE, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['db_id', 'name', 'release_mbid', 'group_mbid', 'error_type'])

            async with aiohttp.ClientSession() as session:
                for idx, r in enumerate(releases, 1):
                    release_mbid = str(r["release_mbid"])
                    group_mbid = str(r["release_group_mbid"])
                    name = r["release_name"]

                    # Progress indicator
                    if idx % 10 == 0:
                        print(f"[{idx}/{total}] Checking: {name}...")

                    # 1. Fetch authoritative release-group for the release
                    if release_mbid in rg_cache:
                        api_group_mbid = rg_cache[release_mbid]
                    else:
                        api_group_mbid = await fetch_release_group(session, release_mbid)
                        rg_cache[release_mbid] = api_group_mbid
                        await asyncio.sleep(1)

                    # 2. Validate & Log
                    if api_group_mbid is None:
                        writer.writerow([r['release_id'], name, release_mbid, group_mbid, 'API_ERROR'])
                    elif api_group_mbid != group_mbid:
                        writer.writerow([r['release_id'], name, release_mbid, group_mbid, 'MISMATCH'])
                        print(f"  ❌ Mismatch found for {name}")
                    
                    # Periodic flush to ensure data is saved to disk
                    if idx % 20 == 0:
                        f.flush()

    print(f"\n✅ Done! Check {LOG_FILE} for results.")

if __name__ == "__main__":
    asyncio.run(main())