"""
Centralized configuration for the music diary application.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class DatabaseConfig:
    """Database connection configuration."""
    DBNAME = os.getenv("DB_NAME", "music_diary")
    USER = os.getenv("DB_USER", "emilynguyen")
    PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("DB_HOST", "localhost")
    PORT = os.getenv("DB_PORT", "5432")
    
    @classmethod
    def as_dict(cls):
        """Return config as dictionary for asyncpg."""
        return {
            "database": cls.DBNAME,
            "user": cls.USER,
            "password": cls.PASSWORD,
            "host": cls.HOST,
            "port": cls.PORT,
        }
    
    @classmethod
    def as_psycopg2_dict(cls):
        """Return config as dictionary for psycopg2."""
        return {
            "dbname": cls.DBNAME,
            "user": cls.USER,
            "password": cls.PASSWORD,
            "host": cls.HOST,
            "port": cls.PORT,
        }


class SpotifyConfig:
    """Spotify API configuration."""
    CLIENT_IDS = os.getenv("SPOTIFY_CLIENT_IDS", "").split(",")
    CLIENT_SECRETS = os.getenv("SPOTIFY_CLIENT_SECRETS", "").split(",")
    TOKEN_URL = "https://accounts.spotify.com/api/token"
    BASE_URL = "https://api.spotify.com/v1"
    
    # Rate limiting
    RATE_LIMIT = 5
    RETRY_LIMIT = 5
    REQUEST_DELAY = 0.5


class MusicBrainzConfig:
    """MusicBrainz API configuration."""
    USER_AGENT = os.getenv("MB_USER_AGENT")
    BASE_URL = "https://musicbrainz.org/ws/2"
    RATE_LIMIT_DELAY = 1.1
    REQUEST_DELAY = 0.3
    RETRY_LIMIT = 5


class ListenBrainzConfig:
    """ListenBrainz API configuration."""
    USERNAME = os.getenv("LISTENBRAINZ_USERNAME", "softpudding000")
    BASE_URL = "https://api.listenbrainz.org/1"
    BATCH_SIZE = 500
    THROTTLE = 1.0
    COMMIT_INTERVAL = 100


class WikidataConfig:
    """Wikidata API configuration."""
    API_URL = "https://www.wikidata.org/w/api.php"
    SPARQL_URL = "https://query.wikidata.org/sparql"


class ValidationConfig:
    """Validation thresholds for fuzzy matching."""
    ARTIST_THRESHOLD = 0.90
    ALBUM_THRESHOLD = 0.90
    TRACK_THRESHOLD = 0.88
    RELEASE_SIMILARITY = 0.80
    
    # metadata refresh intervals
    METADATA_REFRESH_DAYS = 30