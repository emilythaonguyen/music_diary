"""
Matching service for finding best candidates across different music platforms.
NOW WITH UPC PRIORITY FOR RELEASES AND ISNI PRIORITY FOR ARTISTS.
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
    """Service for matching music entities using fuzzy logic with ID prioritization."""
    
    @staticmethod
    def find_best_match(
        db_name: str,
        candidates: List[Dict],
        threshold: float,
        name_field: str = "name",
        db_upc: Optional[str] = None,
        upc_field: Optional[str] = None
    ) -> Optional[str]:
        """
        Find best matching candidate by name similarity, with UPC priority for releases.
        
        Args:
            db_name: Name from database
            candidates: List of candidate dictionaries
            threshold: Minimum similarity threshold
            name_field: Field name containing the candidate name
            db_upc: Optional UPC from database (for release matching)
            upc_field: Optional field name for UPC in candidates
        
        Returns:
            ID of best match or None
        """
        # PRIORITY 1: Exact UPC match (if provided)
        if db_upc and upc_field:
            for candidate in candidates:
                cand_upc = candidate.get(upc_field)
                if cand_upc and cand_upc == db_upc:
                    print(f"✅ Exact UPC match: {db_upc}")
                    return candidate.get("id")
        
        # PRIORITY 2: Name-based fuzzy matching
        db_norm = normalize_title(db_name, aggressive=True)
        db_romaji = normalize_title(transliterate(db_name), aggressive=True)
        
        best_match = None
        best_score = 0.0
        
        for candidate in candidates:
            cand_name = candidate.get(name_field, "")
            score = compute_fuzzy_score(db_norm, db_romaji, cand_name)
            
            effective_threshold = threshold
            if len(db_name) <= 4:
                effective_threshold = 0.98
            
            if score > best_score and score >= effective_threshold:
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
        name_field: str = "name",
        db_upc: Optional[str] = None,
        upc_field: Optional[str] = None
    ) -> List[Dict]:
        """
        Get all candidates above threshold, ranked by similarity, with UPC priority.
        
        Args:
            db_name: Name from database
            candidates: List of candidate dictionaries
            threshold: Minimum similarity threshold
            name_field: Field name containing the candidate name
            db_upc: Optional UPC from database (for release matching)
            upc_field: Optional field name for UPC in candidates
        
        Returns:
            List of matches with scores, sorted by score descending (UPC matches first)
        """
        # Check for exact UPC matches first
        upc_matches = []
        if db_upc and upc_field:
            for candidate in candidates:
                cand_upc = candidate.get(upc_field)
                if cand_upc and cand_upc == db_upc:
                    upc_matches.append({
                        "id": candidate.get("id"),
                        "name": candidate.get(name_field, ""),
                        "score": 1.0,  # Perfect match
                        "data": candidate,
                        "match_type": "upc"
                    })
        
        # Get name-based matches
        db_norm = normalize_title(db_name, aggressive=True)
        db_romaji = normalize_title(transliterate(db_name), aggressive=True)
        
        name_matches = []
        for candidate in candidates:
            cand_name = candidate.get(name_field, "")
            score = compute_fuzzy_score(db_norm, db_romaji, cand_name)
            
            if score >= threshold:
                name_matches.append({
                    "id": candidate.get("id"),
                    "name": cand_name,
                    "score": score,
                    "data": candidate,
                    "match_type": "name"
                })
        
        # Return UPC matches first, then sorted name matches
        return upc_matches + sorted(name_matches, key=lambda x: x["score"], reverse=True)
    
    @staticmethod
    def validate_artist_match(
        db_name: str,
        spotify_name: str,
        aliases: List[str],
        db_isni: Optional[str] = None,
        spotify_isni: Optional[str] = None
    ) -> Tuple[bool, float]:
        """
        Validate if Spotify artist matches database artist, with ISNI priority.
        
        Args:
            db_name: Artist name from database
            spotify_name: Artist name from Spotify
            aliases: List of known aliases for the artist
            db_isni: Optional ISNI from database
            spotify_isni: Optional ISNI from Spotify
        
        Returns:
            Tuple of (is_valid, best_score)
        """
        # PRIORITY 1: Exact ISNI match
        if db_isni and spotify_isni and db_isni == spotify_isni:
            print(f"✅ Exact ISNI match: {db_isni}")
            return True, 1.0
        
        # PRIORITY 2: Name-based matching
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
        spotify_name: str,
        db_upc: Optional[str] = None,
        spotify_upc: Optional[str] = None
    ) -> Tuple[bool, float]:
        """
        Validate if Spotify album matches database release, with UPC priority.
        
        Args:
            db_name: Release name from database
            spotify_name: Album name from Spotify
            db_upc: Optional UPC from database
            spotify_upc: Optional UPC from Spotify
        
        Returns:
            Tuple of (is_valid, score)
        """
        # PRIORITY 1: Exact UPC match
        if db_upc and spotify_upc and db_upc == spotify_upc:
            print(f"✅ Exact UPC match: {db_upc}")
            return True, 1.0
        
        # PRIORITY 2: Name-based matching
        score = similarity_score(db_name, spotify_name, method="validation")
        is_valid = score >= ValidationConfig.ALBUM_THRESHOLD
        
        return is_valid, score
    
    @staticmethod
    def validate_track_match(
        db_name: str,
        spotify_name: str,
        db_isrc: Optional[str] = None,
        spotify_isrc: Optional[str] = None
    ) -> Tuple[bool, float]:
        """
        Validate if Spotify track matches database track, with ISRC priority.
        
        Args:
            db_name: Track name from database
            spotify_name: Track name from Spotify
            db_isrc: Optional ISRC from database
            spotify_isrc: Optional ISRC from Spotify
        
        Returns:
            Tuple of (is_valid, score)
        """
        # PRIORITY 1: Exact ISRC match
        if db_isrc and spotify_isrc and db_isrc == spotify_isrc:
            print(f"✅ Exact ISRC match: {db_isrc}")
            return True, 1.0
        
        # PRIORITY 2: Name-based matching
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