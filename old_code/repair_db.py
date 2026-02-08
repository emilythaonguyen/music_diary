import psycopg2 
import psycopg2.extras
import musicbrainzngs
from dotenv import load_dotenv
import time
import os

load_dotenv()

# database setup
DB_CONFIG = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

# musicbrainz setup
mb_user_agent = os.getenv("MB_USER_AGENT")
if not mb_user_agent:
    raise ValueError("MB_USER_AGENT environment variable is missing!")
musicbrainzngs.set_useragent("ListenBrainzMatcher", "1.0", mb_user_agent)

# SQL query for broken releases
BROKEN_RELEASES_QUERY = """
    SELECT 
        r.release_id,
        r.release_mbid
    FROM releases r
    WHERE 
        r.release_mbid IS NOT NULL 
        AND r.release_mbid != '00000000-0000-0000-0000-000000000000'
        AND r.primary_artist_id NOT IN (SELECT artist_id FROM artists);
"""

def fix_broken_release(cursor, release_data):
    release_id = release_data['release_id']
    release_mbid = release_data['release_mbid']

    try:
        # A. FETCH data from MusicBrainz API
        # We need the 'artist-credits' (for artist MBID) and 'release-groups'
        result = musicbrainzngs.get_release_by_id(
            release_mbid, 
            includes=["artist-credits", "release-groups"]
        )
        
        # Extract the primary artist MBID
        artist_credit = result['release']['artist-credit'][0]
        artist_mbid = artist_credit['artist']['id']
        artist_name = artist_credit['artist']['name']
        
        # Extract the release group MBID
        release_group_mbid = result['release']['release-group']['id']

        # B. FIX ARTIST: Look up or re-create the artist (using the robust logic)
        
        # 1. Try to find the artist by the authoritative MBID
        cursor.execute("SELECT artist_id FROM artists WHERE artist_mbid = %s", (artist_mbid,))
        artist_row = cursor.fetchone()
        
        if artist_row:
            # Artist already exists (e.g., re-created during a previous run)
            new_artist_id = artist_row[0]
        else:
            # Artist is truly missing, re-insert them
            cursor.execute(
                """
                INSERT INTO artists (artist_name, sort_name, artist_mbid, artist_type) 
                VALUES (%s, %s, %s, %s) 
                RETURNING artist_id
                """,
                (artist_name, artist_name, artist_mbid, "Group") # Defaulting to Group/Person requires check
            )
            new_artist_id = cursor.fetchone()[0]

        # C. UPDATE RELEASE: Update the broken release record
        cursor.execute(
            """
            UPDATE releases 
            SET 
                primary_artist_id = %s, 
                release_group_mbid = %s 
            WHERE 
                release_id = %s
            """,
            (new_artist_id, release_group_mbid, release_id)
        )
        print(f"✅ Repaired release ID {release_id}: Linked to new artist ID {new_artist_id}, RG: {release_group_mbid}")
        return True

    except musicbrainzngs.ResponseError as e:
        print(f"❌ MusicBrainz API error for {release_mbid}: {e}")
        return False
    except Exception as e:
        print(f"❌ Database error or unexpected failure for {release_id}: {e}")
        return False

# --- Main Repair Loop ---
def run_repair_script():
    conn = None
    try:
        # 1. Setup Connection
        conn = psycopg2.connect(**DB_CONFIG)
        # Use a dictionary cursor to fetch column names easily
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) 
        
        # 2. Retrieve Targets
        cursor.execute(BROKEN_RELEASES_QUERY)
        # Convert DictCursor results to a standard list of dictionaries
        broken_releases = [dict(row) for row in cursor.fetchall()] 
        
        total_to_repair = len(broken_releases)
        repairs_successful = 0
        print(f"Found {total_to_repair} releases to repair.")

        # 3. Process
        for i, release_data in enumerate(broken_releases):
            print(f"Processing {i + 1}/{total_to_repair}: Release ID {release_data['release_id']}")
            
            # The fix_broken_release function will handle API query and DB updates
            if fix_broken_release(cursor, release_data):
                repairs_successful += 1
            
            # Commit periodically to save progress and release transaction locks
            if (i + 1) % 50 == 0:
                conn.commit()
                print("--- Committed 50 repairs and saved progress ---")
            
            # IMPORTANT: Throttle to respect the MusicBrainz API limit (max 1 request/second)
            time.sleep(1.2) 

        # 4. Cleanup and Final Commit
        conn.commit()
        print(f"\nRepair complete. Success: {repairs_successful} / {total_to_repair}")

    except Exception as e:
        print(f"A fatal error occurred during the repair process: {e}")
        if conn:
            conn.rollback() # Rollback any uncommitted changes if a big error occurs
            print("Database transaction rolled back.")

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    # NOTE: Ensure the fix_broken_release function (from the previous step)
    # is available either in this file or imported correctly.
    run_repair_script()