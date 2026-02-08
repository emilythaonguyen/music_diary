import os
import requests
import time
import re
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================

LISTENBRAINZ_USERNAME = os.getenv("LISTENBRAINZ_USERNAME", "softpudding000")
PROGRESS_FILE = "last_max_ts.txt"
BATCH_SIZE = 500
THROTTLE = 1.0
COMMIT_INTERVAL = 100

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'music_diary'),
    'user': os.getenv('DB_USER', 'emilynguyen'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# ============================================================
# TEXT NORMALIZATION
# ============================================================

def clean_artist_name(name: str) -> str:
    """Remove featuring tags and normalize whitespace."""
    if not name:
        return ""
    # Remove everything after feat/ft
    name = re.split(r'\b(feat\.?|ft\.?)\b', name, flags=re.IGNORECASE)[0]
    # Normalize whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def normalize_text(text: str) -> str:
    """Normalize text for consistent matching."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

# ============================================================
# PROGRESS TRACKING
# ============================================================

def load_progress() -> int:
    """Load the last processed timestamp from file."""
    try:
        with open(PROGRESS_FILE) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return None

def save_progress(max_ts: int):
    """Save the latest processed timestamp to file."""
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(max_ts))

def get_last_listen_ts(cursor) -> int:
    """Get the most recent listen timestamp from database."""
    cursor.execute("SELECT EXTRACT(EPOCH FROM MAX(listened_at))::BIGINT FROM listens")
    row = cursor.fetchone()
    return row[0] if row and row[0] else None

# ============================================================
# DATABASE OPERATIONS (WITH PROPER CONSTRAINT HANDLING)
# ============================================================

def get_or_create_artist(cursor, name: str, mbid: str = None) -> int:
    """
    Get existing artist or create new one.
    Handles artist_mbid uniqueness constraint.
    """
    name = normalize_text(name)
    
    if not name:
        raise ValueError("Artist name cannot be empty")
    
    # Try to find by MBID first (if provided and not null)
    if mbid:
        cursor.execute(
            "SELECT artist_id FROM artists WHERE artist_mbid = %s",
            (mbid,)
        )
        row = cursor.fetchone()
        if row:
            return row[0]
    
    # Try to find by name
    cursor.execute(
        "SELECT artist_id FROM artists WHERE artist_name = %s",
        (name,)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    
    # Create new artist
    try:
        cursor.execute(
            """
            INSERT INTO artists (artist_name, sort_name, artist_type, artist_mbid)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (artist_mbid) DO UPDATE 
                SET artist_name = EXCLUDED.artist_name
            RETURNING artist_id
            """,
            (name, name, "Group", mbid)
        )
        return cursor.fetchone()[0]
    except psycopg2.IntegrityError as e:
        # If there's a constraint violation, try to fetch again
        cursor.execute("ROLLBACK")
        cursor.execute(
            "SELECT artist_id FROM artists WHERE artist_name = %s",
            (name,)
        )
        row = cursor.fetchone()
        if row:
            return row[0]
        raise e

def get_or_create_release(
    cursor, 
    release_name: str, 
    artist_id: int, 
    mbid: str = None
) -> int:
    """
    Get existing release or create new one.
    Handles release_mbid uniqueness constraint.
    """
    release_name = normalize_text(release_name)
    
    if not release_name:
        raise ValueError("Release name cannot be empty")
    
    # Try to find by MBID first (if provided)
    if mbid:
        cursor.execute(
            "SELECT release_id FROM releases WHERE release_mbid = %s",
            (mbid,)
        )
        row = cursor.fetchone()
        if row:
            return row[0]
    
    # Try to find by name + artist
    cursor.execute(
        """
        SELECT release_id 
        FROM releases 
        WHERE release_name = %s AND primary_artist_id = %s
        """,
        (release_name, artist_id)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    
    # Create new release
    try:
        cursor.execute(
            """
            INSERT INTO releases (release_name, primary_artist_id, release_mbid)
            VALUES (%s, %s, %s)
            ON CONFLICT (release_mbid) DO UPDATE
                SET release_name = EXCLUDED.release_name,
                    primary_artist_id = EXCLUDED.primary_artist_id
            RETURNING release_id
            """,
            (release_name, artist_id, mbid)
        )
        return cursor.fetchone()[0]
    except psycopg2.IntegrityError as e:
        # If there's a constraint violation, try to fetch again
        cursor.execute("ROLLBACK")
        cursor.execute(
            """
            SELECT release_id 
            FROM releases 
            WHERE release_name = %s AND primary_artist_id = %s
            """,
            (release_name, artist_id)
        )
        row = cursor.fetchone()
        if row:
            return row[0]
        raise e

def get_or_create_track(
    cursor, 
    track_name: str, 
    release_id: int,
    mbid: str = None
) -> int:
    """
    Get existing track or create new one.
    Note: Tracks don't have uniqueness constraints on recording_mbid.
    """
    track_name = normalize_text(track_name)
    
    if not track_name:
        raise ValueError("Track name cannot be empty")
    
    # Try to find by name + release
    cursor.execute(
        """
        SELECT track_id 
        FROM tracks 
        WHERE track_name = %s AND release_id = %s
        """,
        (track_name, release_id)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    
    # Create new track (no MBID constraint, so simpler)
    cursor.execute(
        """
        INSERT INTO tracks (track_name, release_id, recording_mbid)
        VALUES (%s, %s, %s)
        RETURNING track_id
        """,
        (track_name, release_id, mbid)
    )
    return cursor.fetchone()[0]

def insert_listen(
    cursor,
    listened_at: int,
    track_id: int,
    release_id: int,
    artist_id: int
):
    """
    Insert a listen record.
    Uses ON CONFLICT DO NOTHING to handle the (track_id, listened_at) constraint.
    """
    listened_at_dt = datetime.fromtimestamp(listened_at)
    
    cursor.execute(
        """
        INSERT INTO listens (listened_at, track_id, release_id, artist_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (track_id, listened_at) DO NOTHING
        """,
        (listened_at_dt, track_id, release_id, artist_id)
    )

# ============================================================
# LISTENBRAINZ API
# ============================================================

def fetch_listens(username: str, last_ts: int = None) -> list:
    """
    Fetch all new listens from ListenBrainz API since last_ts.
    Handles pagination and retries.
    """
    url = f"https://api.listenbrainz.org/1/user/{username}/listens"
    all_listens = []
    latest_ts = last_ts

    print(f"📡 Fetching listens for {username} (starting from ts={last_ts or 'beginning'})...")

    while True:
        params = {'count': BATCH_SIZE}
        if latest_ts:
            params['min_ts'] = latest_ts + 1

        # Retry logic
        retries = 0
        while retries < 5:
            try:
                response = requests.get(url, params=params, timeout=15)
                response.raise_for_status()
                
                batch = response.json().get('payload', {}).get('listens', [])

                if not batch:
                    print("✅ No more new listens from API")
                    return all_listens

                all_listens.extend(batch)
                latest_ts = max(l['listened_at'] for l in batch)
                save_progress(latest_ts)

                print(f"  Fetched {len(batch)} listens (Total: {len(all_listens)})")
                time.sleep(THROTTLE)
                break
                
            except (requests.ConnectionError, requests.Timeout) as e:
                retries += 1
                wait_time = 2 ** retries
                print(f"⚠️  Connection error (attempt {retries}/5): {e}")
                print(f"   Retrying in {wait_time}s...")
                time.sleep(wait_time)
                
            except requests.HTTPError as e:
                if e.response.status_code == 429:  # Rate limited
                    wait_time = 60
                    print(f"⚠️  Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    print(f"❌ HTTP error: {e}")
                    return all_listens
                    
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                return all_listens
        else:
            print("❌ Max retries reached")
            break

    return all_listens

# ============================================================
# MAIN PROCESSING
# ============================================================

def process_listen(cursor, listen: dict) -> tuple:
    """
    Process a single listen and insert into database.
    
    Returns:
        tuple: (success: bool, error_message: str or None)
    """
    try:
        track_metadata = listen.get('track_metadata', {})
        
        # Extract data
        artist_name = clean_artist_name(track_metadata.get('artist_name', ''))
        release_name = track_metadata.get('release_name', '')
        track_name = track_metadata.get('track_name', '')
        listened_at = listen.get('listened_at')
        
        # Extract MBIDs if present
        mbids = track_metadata.get('additional_info', {})
        artist_mbid = mbids.get('artist_mbids', [None])[0] if mbids.get('artist_mbids') else None
        release_mbid = mbids.get('release_mbid')
        recording_mbid = mbids.get('recording_mbid')
        
        # Validate required fields
        if not all([artist_name, release_name, track_name, listened_at]):
            return False, "Missing required fields"
        
        # Get or create entities
        artist_id = get_or_create_artist(cursor, artist_name, artist_mbid)
        release_id = get_or_create_release(cursor, release_name, artist_id, release_mbid)
        track_id = get_or_create_track(cursor, track_name, release_id, recording_mbid)
        
        # Insert listen
        insert_listen(cursor, listened_at, track_id, release_id, artist_id)
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def main():
    """Main import process."""
    print("🎵 Starting ListenBrainz import...")
    
    # Connect to database
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False  # Use transactions
    cursor = conn.cursor()
    
    try:
        # Determine starting point
        last_ts = get_last_listen_ts(cursor)
        print(f"📅 Starting from timestamp: {last_ts or 'beginning'}")
        
        # Fetch listens
        listens = fetch_listens(LISTENBRAINZ_USERNAME, last_ts)
        
        if not listens:
            print("ℹ️  No new listens to process")
            return
        
        print(f"\n📝 Processing {len(listens)} listens...")
        
        # Process listens
        success_count = 0
        failed_count = 0
        error_summary = {}
        
        for i, listen in enumerate(listens, 1):
            success, error = process_listen(cursor, listen)
            
            if success:
                success_count += 1
            else:
                failed_count += 1
                error_summary[error] = error_summary.get(error, 0) + 1
            
            # Periodic commits
            if i % COMMIT_INTERVAL == 0:
                conn.commit()
                print(f"  Committed {i}/{len(listens)} listens...")
        
        # Final commit
        conn.commit()
        
        # Print summary
        print("\n" + "="*60)
        print("✅ Import completed!")
        print(f"  Success: {success_count}")
        print(f"  Failed:  {failed_count}")
        
        if error_summary:
            print("\n❌ Error summary:")
            for error, count in sorted(error_summary.items(), key=lambda x: -x[1]):
                print(f"  • {error}: {count}")
        
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        conn.rollback()
        raise
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()