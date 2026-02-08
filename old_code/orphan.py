import asyncio
import asyncpg
import musicbrainzngs
import os
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

musicbrainzngs.set_useragent("MyApp", "1.0", "email@example.com")

async def main():
    pool = await asyncpg.create_pool(**DB_CONFIG)

    orphans = await pool.fetch("""
        SELECT r.release_id, r.release_name, r.release_mbid
        FROM releases r
        LEFT JOIN artists a ON r.primary_artist_id = a.artist_id
        WHERE a.artist_id IS NULL
          AND r.release_mbid IS NOT NULL;
    """)

    print(f"Found {len(orphans)} orphaned releases.")

    for release in tqdm(orphans):
        mbid = str(release["release_mbid"])

        try:
            mb_release = musicbrainzngs.get_release_by_id(
                mbid,
                includes=["artist-credits"]
            )["release"]

            mb_artist = mb_release["artist-credit"][0]["artist"]
            mb_artist_id = mb_artist["id"]
            mb_artist_name = mb_artist["name"]

            # Check if artist exists locally
            local_artist = await pool.fetchrow(
                "SELECT artist_id FROM artists WHERE artist_mbid = $1",
                mb_artist_id
            )

            if not local_artist:
                # Insert missing artist
                local_artist = await pool.fetchrow("""
                    INSERT INTO artists (artist_name, sort_name, artist_mbid, artist_type)
                    VALUES ($1, $1, $2, 'Group')
                    RETURNING artist_id;
                """, mb_artist_name, mb_artist_id)

            # Update release
            await pool.execute("""
                UPDATE releases
                SET primary_artist_id = $1
                WHERE release_id = $2;
            """, local_artist["artist_id"], release["release_id"])

        except Exception as e:
            print(f"❌ Error for release {release['release_name']} ({mbid}): {e}")
    
    # Find orphaned artists in listens
    orphans = await pool.fetch("""
        SELECT DISTINCT l.artist_id, l.release_id
        FROM listens l
        LEFT JOIN artists a ON l.artist_id = a.artist_id
        WHERE a.artist_id IS NULL;
    """)

    print(f"Found {len(orphans)} orphaned artist IDs in listens.")

    for orphan in tqdm(orphans):
        orphan_id = orphan["artist_id"]
        release_id = orphan["release_id"]

        if not release_id:
            continue  # skip if no release info

        # Get release MBID
        release_row = await pool.fetchrow(
            "SELECT release_mbid FROM releases WHERE release_id = $1",
            release_id
        )
        if not release_row or not release_row["release_mbid"]:
            continue

        mbid = str(release_row["release_mbid"])

        try:
            mb_release = musicbrainzngs.get_release_by_id(
                mbid,
                includes=["artist-credits"]
            )["release"]

            # Take primary artist (first in artist-credit)
            mb_artist = mb_release["artist-credit"][0]["artist"]
            mb_artist_id = mb_artist["id"]
            mb_artist_name = mb_artist["name"]

            # Check or create canonical artist locally
            local_artist = await pool.fetchrow(
                "SELECT artist_id FROM artists WHERE artist_mbid = $1",
                mb_artist_id
            )

            if not local_artist:
                local_artist = await pool.fetchrow("""
                    INSERT INTO artists (artist_name, sort_name, artist_mbid, artist_type)
                    VALUES ($1, $1, $2, 'Group')
                    RETURNING artist_id;
                """, mb_artist_name, mb_artist_id)

            # Reassign all listens pointing to orphan_id
            await pool.execute("""
                UPDATE listens
                SET artist_id = $1
                WHERE artist_id = $2;
            """, local_artist["artist_id"], orphan_id)

        except Exception as e:
            print(f"❌ Error for orphaned artist {orphan_id} via release {release_id}: {e}")

    print("Done.")
    
if __name__ == "__main__":
    asyncio.run(main())

