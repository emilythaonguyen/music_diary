import asyncpg
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

DRY_RUN = True   # ⛔ SET TO FALSE TO APPLY CHANGES

ARTIST_PATTERNS = [
    "Black Country%",
    "Tyler%"
]

# --------------------------------------------------

async def pick_canonical_artist(pool, artist_ids):
    rows = await pool.fetch("""
        SELECT
            a.artist_id,
            a.artist_name,
            COUNT(DISTINCT r.release_id) AS release_count,
            COUNT(DISTINCT t.track_id)   AS track_count,
            COUNT(DISTINCT l.listen_id)  AS listen_count,
            a.spotify_id,
            a.artist_mbid
        FROM artists a
        LEFT JOIN releases r ON r.primary_artist_id = a.artist_id
        LEFT JOIN tracks t ON t.release_id = r.release_id
        LEFT JOIN listens l ON l.track_id = t.track_id
        WHERE a.artist_id = ANY($1)
        GROUP BY a.artist_id
        ORDER BY
            release_count DESC,
            track_count DESC,
            listen_count DESC,
            (a.spotify_id IS NOT NULL) DESC,
            (a.artist_mbid IS NOT NULL) DESC
        LIMIT 1;
    """, artist_ids)

    return rows[0]


async def merge_tracks(pool, keep_release, drop_release):
    # Merge tracks by recording_mbid
    rows = await pool.fetch("""
        SELECT t1.track_id AS keep_track,
               t2.track_id AS drop_track
        FROM tracks t1
        JOIN tracks t2
          ON t1.recording_mbid IS NOT NULL
         AND t1.recording_mbid = t2.recording_mbid
         AND t1.release_id = $1
         AND t2.release_id = $2
    """, keep_release, drop_release)

    for r in rows:
        print(f"    🎧 Track merge {r['drop_track']} → {r['keep_track']}")
        if not DRY_RUN:
            await pool.execute(
                "UPDATE listens SET track_id=$1 WHERE track_id=$2",
                r["keep_track"], r["drop_track"]
            )
            await pool.execute(
                "DELETE FROM tracks WHERE track_id=$1",
                r["drop_track"]
            )

    # Move remaining tracks
    print(f"    🎵 Moving tracks from release {drop_release} → {keep_release}")
    if not DRY_RUN:
        await pool.execute(
            "UPDATE tracks SET release_id=$1 WHERE release_id=$2",
            keep_release, drop_release
        )


async def merge_releases(pool, canonical_artist, duplicate_artist):
    releases = await pool.fetch("""
        SELECT release_id, release_mbid, release_name
        FROM releases
        WHERE primary_artist_id = $1
    """, duplicate_artist)

    for r in releases:
        dup_release = r["release_id"]

        keep = await pool.fetchrow("""
            SELECT r.release_id
            FROM releases r
            LEFT JOIN tracks t ON t.release_id = r.release_id
            LEFT JOIN listens l ON l.release_id = r.release_id
            WHERE r.primary_artist_id = $1
            AND r.release_mbid = $2
            GROUP BY r.release_id
            ORDER BY
                COUNT(DISTINCT t.track_id) DESC,
                COUNT(DISTINCT l.listen_id) DESC
            LIMIT 1;
        """, canonical_artist, r["release_mbid"])

        if keep:
            print(f"  💿 Merge release '{r['release_name']}' ({dup_release} → {keep['release_id']})")
            await merge_tracks(pool, keep["release_id"], dup_release)

            if not DRY_RUN:
                await pool.execute(
                    "UPDATE listens SET release_id=$1 WHERE release_id=$2",
                    keep["release_id"], dup_release
                )
                await pool.execute(
                    "DELETE FROM releases WHERE release_id=$1",
                    dup_release
                )
        else:
            print(f"  💿 Reassign release '{r['release_name']}' → artist {canonical_artist}")
            if not DRY_RUN:
                await pool.execute(
                    "UPDATE releases SET primary_artist_id=$1 WHERE release_id=$2",
                    canonical_artist, dup_release
                )


async def merge_artist_group(pool, artists):
    artist_ids = [a["artist_id"] for a in artists]
    canonical = await pick_canonical_artist(pool, artist_ids)

    print(f"\n🎤 Canonical artist: {canonical['artist_name']} ({canonical['artist_id']})")

    for a in artists:
        if a["artist_id"] == canonical["artist_id"]:
            continue

        print(f"➡️  Merging artist {a['artist_name']} ({a['artist_id']})")
        await merge_releases(pool, canonical["artist_id"], a["artist_id"])

        if not DRY_RUN:
            await pool.execute(
                "DELETE FROM artists WHERE artist_id=$1",
                a["artist_id"]
            )


async def main():
    pool = await asyncpg.create_pool(**DB_CONFIG)

    for pattern in ARTIST_PATTERNS:
        print(f"\n🔍 Processing artists LIKE '{pattern}'")
        artists = await pool.fetch("""
            SELECT artist_id, artist_name
            FROM artists
            WHERE artist_name ILIKE $1
            ORDER BY artist_id
        """, pattern)

        if len(artists) <= 1:
            print("  ✔ No duplicates found")
            continue

        await merge_artist_group(pool, artists)

    await pool.close()

    print("\n✅ Done")
    if DRY_RUN:
        print("⚠️ DRY RUN — no changes applied")


if __name__ == "__main__":
    asyncio.run(main())
