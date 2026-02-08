"""
MusicBrainz API client with rate limiting and retry logic.
"""
import asyncio
import functools
from typing import Optional, Dict, Any, List

import musicbrainzngs

from core.config import MusicBrainzConfig


# initialize musicbrainz library
if MusicBrainzConfig.USER_AGENT:
    musicbrainzngs.set_useragent(
        "MusicDiary",
        "1.0",
        MusicBrainzConfig.USER_AGENT
    )


class MusicBrainzClient:
    """
    Async MusicBrainz API client.
    Wraps the blocking musicbrainzngs library with async/await.
    """
    
    @staticmethod
    async def _safe_call(
        func: callable,
        *args,
        retries: int = 5,
        delay: float = 1.0,
        **kwargs
    ) -> Optional[Dict]:
        """
        Async wrapper for blocking MusicBrainz calls with retry logic.
        
        Args:
            func: MusicBrainz function to call
            args: Positional arguments
            retries: Number of retries
            delay: Initial delay between retries (doubles each retry)
            kwargs: Keyword arguments
        
        Returns:
            API response or None if failed
        """
        loop = asyncio.get_event_loop()
        
        for attempt in range(retries):
            try:
                result = await loop.run_in_executor(
                    None,
                    functools.partial(func, *args, **kwargs)
                )
                return result
            except Exception as e:
                if attempt < retries - 1:
                    print(f"⚠️ MB request failed: {e}. Retry {attempt + 1}/{retries} in {delay}s")
                    await asyncio.sleep(delay)
                    delay *= 2
                else:
                    print(f"❌ MB request failed after {retries} attempts")
                    return None
    
    async def search_artists(
        self,
        query: str,
        limit: int = 25
    ) -> List[Dict]:
        """
        Search for artists on MusicBrainz.
        
        Args:
            query: Artist name to search
            limit: Maximum results
        
        Returns:
            List of artist objects
        """
        result = await self._safe_call(
            musicbrainzngs.search_artists,
            query,
            limit=limit
        )
        
        if result and "artist-list" in result:
            return result["artist-list"]
        return []
    
    async def search_releases(
        self,
        query: str,
        artist: Optional[str] = None,
        limit: int = 25
    ) -> List[Dict]:
        """
        Search for releases on MusicBrainz.
        
        Args:
            query: Release name to search
            artist: Artist name to filter by
            limit: Maximum results
        
        Returns:
            List of release objects
        """
        result = await self._safe_call(
            musicbrainzngs.search_releases,
            query,
            artist=artist,
            limit=limit
        )
        
        if result and "release-list" in result:
            return result["release-list"]
        return []
    
    async def get_artist(
        self,
        mbid: str,
        includes: Optional[List[str]] = None
    ) -> Optional[Dict]:
        """
        Get artist details by MBID.
        
        Args:
            mbid: MusicBrainz ID
            includes: List of additional data to include
        
        Returns:
            Artist object or None
        """
        includes = includes or []
        result = await self._safe_call(
            musicbrainzngs.get_artist_by_id,
            mbid,
            includes=includes
        )
        
        await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
        
        if result and "artist" in result:
            return result["artist"]
        return None
    
    async def get_release(
        self,
        mbid: str,
        includes: Optional[List[str]] = None
    ) -> Optional[Dict]:
        """
        Get release details by MBID.
        
        Args:
            mbid: MusicBrainz ID
            includes: List of additional data to include
        
        Returns:
            Release object or None
        """
        includes = includes or []
        result = await self._safe_call(
            musicbrainzngs.get_release_by_id,
            mbid,
            includes=includes
        )
        
        await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
        
        if result and "release" in result:
            return result["release"]
        return None
    
    async def get_release_group(
        self,
        mbid: str,
        includes: Optional[List[str]] = None
    ) -> Optional[Dict]:
        """
        Get release group details by MBID.
        
        Args:
            mbid: MusicBrainz ID
            includes: List of additional data to include
        
        Returns:
            Release group object or None
        """
        includes = includes or []
        result = await self._safe_call(
            musicbrainzngs.get_release_group_by_id,
            mbid,
            includes=includes
        )
        
        await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
        
        if result and "release-group" in result:
            return result["release-group"]
        return None
    
    async def get_recording(
        self,
        mbid: str,
        includes: Optional[List[str]] = None
    ) -> Optional[Dict]:
        """
        Get recording details by MBID.
        
        Args:
            mbid: MusicBrainz ID
            includes: List of additional data to include
        
        Returns:
            Recording object or None
        """
        includes = includes or []
        result = await self._safe_call(
            musicbrainzngs.get_recording_by_id,
            mbid,
            includes=includes
        )
        
        await asyncio.sleep(MusicBrainzConfig.RATE_LIMIT_DELAY)
        
        if result and "recording" in result:
            return result["recording"]
        return None
    
    @staticmethod
    def extract_spotify_id(url_relations: List[Dict]) -> Optional[str]:
        """
        Extract Spotify ID from MusicBrainz URL relations.
        
        Args:
            url_relations: List of URL relation objects
        
        Returns:
            Spotify ID or None
        """
        for rel in url_relations:
            target = rel.get("target", "")
            if "open.spotify.com" in target:
                # remove trailing slashes and query params
                clean_url = target.split("?")[0].strip("/")
                
                parts = clean_url.split("/")
                if parts:
                    spotify_id = parts[-1]

                    # spotify ids have 22 characters
                    if len(spotify_id) == 22:
                        return spotify_id
        
        return None