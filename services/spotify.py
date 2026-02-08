"""
Spotify API client with authentication, rate limiting, and credential rotation.
"""
import asyncio
import time
from itertools import cycle
from typing import Optional, Dict, List

import aiohttp

from core.config import SpotifyConfig


class SpotifyClient:
    """
    Async Spotify API client with:
    - OAuth token management
    - Multiple credential rotation
    - Rate limiting and retry logic
    - Request semaphore for concurrency control
    """
    
    def __init__(self):
        """Initialize Spotify client with credential rotation."""
        self.client_cycle = cycle(
            zip(SpotifyConfig.CLIENT_IDS, SpotifyConfig.CLIENT_SECRETS)
        )
        self.current_id, self.current_secret = next(self.client_cycle)
        self.token_data: Optional[Dict] = None
        self.token_expires: float = 0
        self.blocked_until: Dict[str, float] = {}
        self.semaphore = asyncio.Semaphore(SpotifyConfig.RATE_LIMIT)
    
    async def get_token(self, session: aiohttp.ClientSession) -> str:
        """
        Get or refresh Spotify access token.
        
        Args:
            session: aiohttp session
        
        Returns:
            Access token string
        """
        # return existing token if still valid
        if self.token_data and time.time() < self.token_expires - 60:
            return self.token_data["access_token"]
        
        # request new token
        async with session.post(
            SpotifyConfig.TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=aiohttp.BasicAuth(self.current_id, self.current_secret),
        ) as resp:
            self.token_data = await resp.json()
            self.token_expires = time.time() + self.token_data.get("expires_in", 3600)
            return self.token_data["access_token"]
    
    def rotate_credentials(self) -> bool:
        """
        Switch to next available API credentials.
        
        Returns:
            True if rotation successful, False if all credentials blocked
        """
        for _ in range(len(SpotifyConfig.CLIENT_IDS)):
            cid, secret = next(self.client_cycle)
            if self.blocked_until.get(cid, 0) < time.time():
                self.current_id, self.current_secret = cid, secret
                self.token_data = None
                self.token_expires = 0
                print(f"✓ Rotated to Spotify app {self.current_id[:8]}...")
                return True
        return False
    
    async def handle_rate_limit(self, retry_after: int) -> None:
        """
        Handle rate limit by blocking current credentials and rotating.
        
        Args:
            retry_after: Seconds to wait before retrying
        """
        print(f"⚠️ Rate limit for {self.current_id[:8]}: {retry_after//60}min cooldown")
        self.blocked_until[self.current_id] = time.time() + retry_after
        
        rotated = self.rotate_credentials()
        if rotated:
            await asyncio.sleep(5)
        else:
            # all apps blocked - wait for shortest block
            wait_time = min(
                ts - time.time()
                for ts in self.blocked_until.values()
                if ts > time.time()
            )
            print(f"⏳ All apps blocked. Waiting {int(wait_time)}s...")
            await asyncio.sleep(wait_time + 1)
    
    async def request(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        params: Optional[Dict] = None
    ) -> Optional[Dict]:
        """
        Make a safe Spotify API request with retries and rate limiting.
        
        Args:
            session: aiohttp session
            endpoint: API endpoint (e.g., "/artists/123")
            params: Query parameters
        
        Returns:
            JSON response or None if failed
        """
        async with self.semaphore:
            retries = 0
            backoff = 1
            
            while retries < SpotifyConfig.RETRY_LIMIT:
                try:
                    token = await self.get_token(session)
                    headers = {"Authorization": f"Bearer {token}"}
                    
                    async with session.get(
                        f"{SpotifyConfig.BASE_URL}{endpoint}",
                        headers=headers,
                        params=params
                    ) as resp:
                        if resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", 5))
                            if retry_after > 600:
                                await self.handle_rate_limit(retry_after)
                                continue
                            else:
                                await asyncio.sleep(retry_after + 1)
                        
                        elif resp.status in (500, 502, 503):
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 60)
                        
                        elif resp.status == 400:
                            text = await resp.text()
                            print(f"⚠️ Spotify 400: {text}")
                            return None
                        
                        elif resp.status != 200:
                            text = await resp.text()
                            print(f"⚠️ Spotify {resp.status}: {text}")
                            return None
                        
                        else:
                            return await resp.json()
                
                except Exception as e:
                    print(f"⚠️ Request error: {e}. Retrying in {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                
                retries += 1
            
            print(f"❌ Max retries for {endpoint}")
            return None
    
    async def search_artist(
        self,
        session: aiohttp.ClientSession,
        artist_name: str,
        limit: int = 10
    ) -> List[Dict]:
        """
        Search for artists on Spotify.
        
        Args:
            session: aiohttp session
            artist_name: Artist name to search
            limit: Maximum results
        
        Returns:
            List of artist objects
        """
        result = await self.request(
            session,
            "/search",
            params={"q": artist_name, "type": "artist", "limit": limit}
        )
        return result.get("artists", {}).get("items", []) if result else []
    
    async def fetch_paginated(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        params: Optional[Dict] = None,
        max_pages: int = 10
    ) -> List[Dict]:
        """
        Fetch paginated results from Spotify API.
        
        Args:
            session: aiohttp session
            endpoint: API endpoint
            params: Query parameters
            max_pages: Maximum pages to fetch
        
        Returns:
            List of all items from all pages
        """
        all_items = []
        params = params or {}
        url = endpoint
        
        for _ in range(max_pages):
            if not url:
                break
            
            result = await self.request(session, url, params)
            if not result or "items" not in result:
                break
            
            all_items.extend(result["items"])
            
            # get next page
            url = result.get("next")
            if url:
                # strip base url to get endpoint
                url = url.replace(SpotifyConfig.BASE_URL, "")
            params = None  # params included in next URL
        
        return all_items