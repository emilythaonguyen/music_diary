import asyncio
from typing import Optional, Dict

import aiohttp

from core.config import LastFMConfig

class LastFMClient:
    """
    Async Last.fm API client with:
    - Rate limiting and retry logic
    - Request semaphore for concurrency control
    """
    
    def __init__(self):
        """Initilize Last.fm client"""
        self.api_key = LastFMConfig.API_KEY
        self.base_url = LastFMConfig.BASE_URL
        self.semaphore = asyncio.Semaphore(5)
        
    async def request(
        self,
        session: aiohttp.ClientSession,
        method: str,
        params: Optional[Dict] = None
    ) -> Optional[Dict]:
        """
        Make a safe Last.fm API request with retries and rate limiting.

        Args:
            session: aiohttp session
            method: API method (e.g., "artist.getInfo")
            params: Query parameters

        Returns:
            JSON response or None if failed
        """
        async with self.semaphore:
            retries = 0
            backoff = 1
            
            req_params = {
                "method": method,
                "api_key": self.api_key,
                "format": "json"
            }
            if params:
                req_params.update(params)
                
            headers = {"User-Agent": LastFMConfig.USER_AGENT} if LastFMConfig.USER_AGENT else {}
            
            while retries < 5:
                try:
                    async with session.get(
                        self.base_url,
                        params=req_params,
                        headers=headers
                    ) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        
                        elif resp.status in (429, 500, 502, 503):
                            await asyncio.sleep(backoff)
                            backoff = min(backoff *2, 60)
                            
                        else:
                            text = await resp.text()
                            print(f"⚠️ Last.fm {resp.status}: {text}")
                            return None
                    
                except Exception as e:
                    print(f"⚠️ Request error: {e}. Retrying in {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                
                retries += 1
                
            print(f"❌ Max retries for method {method}")
            return None
        
    async def get_artist_info(
        self,
        session: aiohttp.ClientSession,
        artist: str,
        mbid: Optional[str] = None
    ) -> Optional[Dict]:
        """Fetch artist details from Last.fm."""
        params = {"mbid": mbid} if mbid else {"artist": artist}
        return await self.request(session, "artist.getInfo", params)
    
    async def get_album_info(
        self,
        session: aiohttp.ClientSession,
        artist: str,
        album: str,
        mbid: Optional[str] = None
    ) -> Optional[Dict]:
        """Fetch album details from Last.fm."""
        params = {"mbid": mbid} if mbid else {"artist": artist, "album": album}
        return await self.request(session, "album.getInfo", params)
    
    async def get_track_info(
        self,
        session: aiohttp.ClientSession,
        artist: str,
        track: str,
        mbid: Optional[str] = None
    ) -> Optional[Dict]:
        """Fetch track details from Last.fm."""
        params = {"mbid": mbid} if mbid else {"artist": artist, "track": track}
        return await self.request(session, "track.getInfo", params)
    
    