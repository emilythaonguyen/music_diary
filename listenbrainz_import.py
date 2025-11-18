import os
import requests
import time
import re
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

LISTENBRAINZ_USERNAME = os.getenv("LISTENBRAINZ_USERNAME", "softpudding000")
PROGRESS_FILE = "last_max_ts.txt"
BATCH_SIZE = 500
THROTTLE = 1.0

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'music_diary'),
    'user': os.getenv('DB_USER', 'emilynguyen'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# ---------------- utilities ----------------
def clean_artist_name(name: str) -> str:
    if not name:
        return ""
    name = re.split(r'\b(feat\.?|ft)\b', name, flags=re.IGNORECASE)[0]
    name = re.split(r'[,]', name)[0]
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def load_progress():
    try:
        with open(PROGRESS_FILE) as f:
            return int(f.read())
    except FileNotFoundError:
        return None

def save_progress(max_ts):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(max_ts))
        
def get_last_listen_ts(cursor):
    cursor.execute("SELECT EXTRACT(EPOCH FROM MAX(listened_at))::BIGINT FROM listens")
    row = cursor.fetchone()
    return row[0] if row and row[0] else None

# ---------------- Database helpers ----------------
def get_or_create_artist(cursor, name):
    cursor.execute("SELECT artist_id FROM artists WHERE artist_name = %s", (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO artists (artist_name, sort_name, artist_type) VALUES (%s, %s, %s) RETURNING artist_id",
        (name, name, "Group")
    )
    return cursor.fetchone()[0]

def get_or_create_release(cursor, name, artist_id):
    cursor.execute(
        "SELECT release_id FROM releases WHERE release_name = %s AND primary_artist_id = %s",
        (name, artist_id)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO releases (release_name, primary_artist_id) VALUES (%s, %s) RETURNING release_id",
        (name, artist_id)
    )
    return cursor.fetchone()[0]

def get_or_create_track(cursor, name, release_id):
    cursor.execute(
        "SELECT track_id FROM tracks WHERE track_name = %s AND release_id = %s",
        (name, release_id)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO tracks (track_name, release_id) VALUES (%s, %s) RETURNING track_id",
        (name, release_id)
    )
    return cursor.fetchone()[0]

def insert_listen(cursor, listen, artist_id, release_id, track_id):
    listened_at = datetime.fromtimestamp(listen.get('listened_at'))
    cursor.execute(
        """
        INSERT INTO listens (listened_at, track_id, release_id, artist_id)
        VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
        """,
        (listened_at, track_id, release_id, artist_id)
    )

# ---------------- fetch listens ----------------
def fetch_listens(username, last_ts=None):
    """
    Fetch new listens from ListenBrainz for a user.
    Uses min_ts to fetch listens newer than last fetched timestamp.
    """
    url = f"https://api.listenbrainz.org/1/user/{username}/listens"
    all_listens = []
    latest_ts = last_ts  # timestamp of the most recent listen we have saved

    while True:
        params = {'count': BATCH_SIZE}
        if latest_ts:
            # fetch listens **after** the last known timestamp
            params['min_ts'] = latest_ts + 1

        retries = 0
        while retries < 5:
            try:
                response = requests.get(url, params=params, timeout=15)
                response.raise_for_status()
                batch = response.json().get('payload', {}).get('listens', [])
                
                if not batch:
                    print("No more new listens returned by API.")
                    return all_listens

                all_listens.extend(batch)
                # update latest_ts to the newest listen in this batch
                latest_ts = max(l['listened_at'] for l in batch)
                save_progress(latest_ts)

                print(f"Fetched {len(batch)} listens (Total: {len(all_listens)})")
                time.sleep(THROTTLE)
                break  # exit retry loop
            except (requests.ConnectionError, requests.Timeout) as e:
                retries += 1
                wait_time = 2 ** retries
                print(f"Connection error (attempt {retries}/5): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                print(f"Unexpected error: {e}")
                return all_listens
        else:
            print("Max retries reached, stopping fetch.")
            break

    return all_listens

# ---------------- main ----------------
def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    last_ts = get_last_listen_ts(cursor)
    print("Starting from timestamp:", last_ts)
    
    listens = fetch_listens(LISTENBRAINZ_USERNAME, last_ts)

    success = 0
    failed = 0

    for listen in listens:
        track_metadata = listen.get('track_metadata', {})
        artist_name = clean_artist_name(track_metadata.get('artist_name'))
        release_name = track_metadata.get('release_name')
        track_name = track_metadata.get('track_name')

        if not all([artist_name, release_name, track_name]):
            failed += 1
            continue

        try:
            artist_id = get_or_create_artist(cursor, artist_name)
            release_id = get_or_create_release(cursor, release_name, artist_id)
            track_id = get_or_create_track(cursor, track_name, release_id)
            insert_listen(cursor, listen, artist_id, release_id, track_id)
            success += 1
        except Exception as e:
            print(f"Error inserting listen: {e}")
            failed += 1
            continue

        if success % 100 == 0:
            conn.commit()
            print(f"Committed {success} listens so far...")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Finished. Success: {success}, Failed: {failed}")

if __name__ == "__main__":
    main()
