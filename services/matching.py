"""
Matching service for finding best candidates across different music platforms.
"""
from typing import List, Dict, Optional, Tuple

from utils.text import (
    normalize_title,
    transliterate,
    compute_fuzzy_score,
    similarity_score
)
from core.config import ValidationConfig


class EntityMatcher:
    """Service for matching music entities using fuzzy logic."""
    
    @staticmethod
    def find_best_match(
        db_name: str,
        candidates: List[Dict],
        threshold: float,
        name_field: str = "name"
    ) -> Optional[str]:
        """
        Find best matching candidate by name similarity.
        
        Args:
            db_name: Name from database
            candidates: List of candidate dictionaries
            threshold: Minimum similarity threshold
            name_field: Field name containing the candidate name
        
        Returns:
            ID of best match or None
        """
        db_norm = normalize_title(db_name, aggressive=True)
        db_romaji = normalize_title(transliterate(db_name), aggressive=True)
        
        best_match = None
        best_score = 0.0
        
        for candidate in candidates:
            cand_name = candidate.get(name_field, "")
            score = compute_fuzzy_score(db_norm, db_romaji, cand_name)
            
            if score > best_score:
                best_score = score
                best_match = candidate
        
        if best_match and best_score >= threshold:
            return best_match.get("id")
        
        return None
    
    @staticmethod
    def get_ranked_matches(
        db_name: str,
        candidates: List[Dict],
        threshold: float = 0.80,
        name_field: str = "name"
    ) -> List[Dict]:
        """
        Get all candidates above threshold, ranked by similarity.
        
        Args:
            db_name: Name from database
            candidates: List of candidate dictionaries
            threshold: Minimum similarity threshold
            name_field: Field name containing the candidate name
        
        Returns:
            List of matches with scores, sorted by score descending
        """
        db_norm = normalize_title(db_name, aggressive=True)
        db_romaji = normalize_title(transliterate(db_name), aggressive=True)
        
        matches = []
        for candidate in candidates:
            cand_name = candidate.get(name_field, "")
            score = compute_fuzzy_score(db_norm, db_romaji, cand_name)
            
            if score >= threshold:
                matches.append({
                    "id": candidate.get("id"),
                    "name": cand_name,
                    "score": score,
                    "data": candidate
                })
        
        return sorted(matches, key=lambda x: x["score"], reverse=True)
    
    @staticmethod
    def validate_artist_match(
        db_name: str,
        spotify_name: str,
        aliases: List[str]
    ) -> Tuple[bool, float]:
        """
        Validate if Spotify artist matches database artist or aliases.
        
        Args:
            db_name: Artist name from database
            spotify_name: Artist name from Spotify
            aliases: List of known aliases for the artist
        
        Returns:
            Tuple of (is_valid, best_score)
        """
        # compare db name
        scores = [
            similarity_score(db_name, spotify_name, method="validation")
        ]
        
        # compare aliases
        scores.extend(
            similarity_score(alias, spotify_name, method="validation")
            for alias in aliases
        )
        
        best_score = max(scores)
        is_valid = best_score >= ValidationConfig.ARTIST_THRESHOLD
        
        return is_valid, best_score
    
    @staticmethod
    def validate_release_match(
        db_name: str,
        spotify_name: str
    ) -> Tuple[bool, float]:
        """
        Validate if Spotify album matches database release.
        
        Args:
            db_name: Release name from database
            spotify_name: Album name from Spotify
        
        Returns:
            Tuple of (is_valid, score)
        """
        score = similarity_score(db_name, spotify_name, method="validation")
        is_valid = score >= ValidationConfig.ALBUM_THRESHOLD
        
        return is_valid, score
    
    @staticmethod
    def validate_track_match(
        db_name: str,
        spotify_name: str
    ) -> Tuple[bool, float]:
        """
        Validate if Spotify track matches database track.
        
        Args:
            db_name: Track name from database
            spotify_name: Track name from Spotify
        
        Returns:
            Tuple of (is_valid, score)
        """
        score = similarity_score(db_name, spotify_name, method="validation")
        is_valid = score >= ValidationConfig.TRACK_THRESHOLD
        
        return is_valid, score
    
    @staticmethod
    async def validate_artist_with_releases(
        session,
        spotify_client,
        spotify_id: str,
        db_releases: List[str],
        min_score: float = 0.90
    ) -> bool:
        """
        Validate artist match by checking if they have similar releases.
        
        Args:
            session: aiohttp session
            spotify_client: SpotifyClient instance
            spotify_id: Spotify artist ID
            db_releases: List of release names from database
            min_score: Minimum similarity score
        
        Returns:
            True if validation passes
        """
        if not db_releases:
            return True
        
        # fetch artist's albums from spotify
        albums = await spotify_client.request(
            session,
            f"/artists/{spotify_id}/albums",
            {"album_type": "album,single", "limit": 50}
        )
        
        if not albums or "items" not in albums:
            return False
        
        # preprocess db release names
        db_processed = [
            (
                normalize_title(title, aggressive=True),
                normalize_title(transliterate(title), aggressive=True)
            )
            for title in db_releases
        ]
        
        # check if any spotify album matches any db release
        for album in albums["items"]:
            sp_name = album.get("name")
            if not sp_name:
                continue
            
            for db_norm, db_romaji in db_processed:
                if compute_fuzzy_score(db_norm, db_romaji, sp_name) >= min_score:
                    return True
        
        return False