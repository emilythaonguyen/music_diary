"""
Validate Spotify matches by checking if artist/album/track names match.
Generates CSV reports of mismatches for manual review.
"""
import asyncio
import csv
from typing import List

import aiohttp
import asyncpg

from core.config import DatabaseConfig, SpotifyConfig
from services.spotify import SpotifyClient
from services.matching import EntityMatcher

from services.musicbrainz import MusicBrainzClient


class SpotifyValidator:
    """Validate Spotify matches."""
    
    def __init__(self, pool: asyncpg.Pool, output_file: str = "spotify_mismatches.csv"):
        """
        Initialize validator.
        
        Args:
            pool: Database connection pool
            output_file: Path to CSV output file
        """
        self.pool = pool
        self.spotify = SpotifyClient()
        self.matcher = EntityMatcher()
        self.output_file = output_file
        self.mismatches = []
        self.mb_client = MusicBrainzClient()
    
    def log_mismatch(
        self,
        entity_type: str,
        entity_id: int,
        db_name: str,
        spotify_name: str,
        score: float,
        error_type: str = "mismatch"
    ) -> None:
        """
        Log a validation mismatch.
        
        Args:
            entity_type: Type of entity (artist/album/track)
            entity_id: Database ID
            db_name: Name in database
            spotify_name: Name from Spotify
            score: Similarity score
            error_type: Type of error
        """
        self.mismatches.append({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "db_name": db_name,
            "spotify_name": spotify_name,
            "score": f"{score:.2f}",
            "error_type": error_type
        })
        print(f"⚠️ {entity_type.capitalize()} '{db_name}' mismatch: Spotify='{spotify_name}' score={score:.2f}")
    
    async def get_artist_aliases(self, artist_id: int) -> List[str]:
        """Get all aliases for an artist."""
        rows = await self.pool.fetch(
            "SELECT alias_name FROM artist_aliases WHERE artist_id = $1",
            artist_id
        )
        return [r["alias_name"] for r in rows]
    
    async def validate_artists(self, session: aiohttp.ClientSession) -> int:
        """
        Validate all artist matches using Spotify name matching and
        MusicBrainz official URL relationships.
        
        Args:
            session: aiohttp session
        
        Returns:
            Number of mismatches found
        """
        artists = await self.pool.fetch(
            """
            SELECT artist_id, artist_name, sort_name, artist_type, spotify_id, artist_mbid
            FROM artists
            WHERE spotify_id IS NOT NULL AND on_spotify IS TRUE
            """
        )
        
        print(f"\n🔍 Validating {len(artists)} artists...")
        mismatches = 0
        
        for artist in artists:
            spotify_id = artist["spotify_id"]
            artist_mbid = artist["artist_mbid"]
            artist_id = artist["artist_id"]
            db_name = artist["artist_name"]
                            
            if not isinstance(spotify_id, str) or len(spotify_id) != 22:
                self.log_mismatch(
                    "artist",
                    artist_id,
                    db_name,
                    "N/A",
                    0.0,
                    "malformed_id"
                )
                mismatches += 1
                continue
            
            # validate musicbrainz spotify url relationship
            if artist_mbid:
                try:
                    mb_artist = await self.mb_client.get_artist(artist_mbid, includes=["url-rels"])
                    
                    if mb_artist:
                        relations = mb_artist.get("url-relation-list", [])
                        official_spotify_id = self.mb_client.extract_spotify_id(relations)
                        
                        if official_spotify_id and spotify_id != official_spotify_id:
                            self.log_mismatch(
                                "artist", 
                                artist_id, 
                                db_name, 
                                f"DB: {spotify_id} | MB Official: {official_spotify_id}", 
                                0.0, 
                                "mb_relationship_mismatch"
                            )
                            mismatches += 1
                            
                            async with self.pool.acquire() as conn:
                                await conn.execute(
                                    "UPDATE artists SET spotify_id = NULL, on_spotify = NULL WHERE artist_id = $1", 
                                    artist["artist_id"]
                                )
                            print(f"  → Nulled Spotify ID (MB Relationship Mismatch)")
                            continue
                        
                except Exception as e:
                    print(f"  ⚠️ Skipping MB check for {db_name}: {e}")
            
            else:
                pass
            
            # fetch from spotify
            spotify_artist = await self.spotify.request(
                session,
                f"/artists/{spotify_id}"
            )
            
            if not spotify_artist:
                self.log_mismatch(
                    "artist",
                    artist_id,
                    db_name,
                    "N/A",
                    0.0,
                    "not_found"
                )
                mismatches += 1
                continue
            
            spotify_name = spotify_artist.get("name")
            if not spotify_name:
                self.log_mismatch(
                    "artist",
                    artist_id,
                    db_name,
                    "N/A",
                    0.0,
                    "missing_name"
                )
                mismatches += 1
                continue
            
            # get aliases
            aliases = await self.get_artist_aliases(artist["artist_id"])
            
            if db_name.lower() == spotify_name.lower():
                continue
            
            # validate match
            is_valid, score = self.matcher.validate_artist_match(
                db_name,
                spotify_name,
                aliases
            )
            
            if is_valid:
                continue
            
            # release validation
            if score >= 0.75:
                db_releases = await self.get_artist_releases(artist_id)
                is_valid_by_releases = await self.matcher.validate_artist_with_releases(
                    session, 
                    self.spotify,
                    spotify_id,
                    db_releases,
                    min_score=0.8
                )
                
                if is_valid_by_releases:
                    print(f"  → Verified '{db_name}' via release match (Score: {score:.2f})")
                    continue
                
            # null out very bad matches (score < 0.75)
            self.log_mismatch("artist",
                              artist_id, 
                              db_name,
                              spotify_name,
                              score, 
                              "COMPLETE_MISMATCH"
            )
            mismatches += 1
            
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE artists SET spotify_id = NULL, on_spotify = NULL WHERE artist_id = $1",
                    artist["artist_id"]
                )
            print(f"  → Nulled Spotify ID (score too low)")
           
            await asyncio.sleep(SpotifyConfig.REQUEST_DELAY)
        
        return mismatches
    
    async def validate_albums(self, session: aiohttp.ClientSession) -> int:
        """
        Validate all album matches.
        
        Args:
            session: aiohttp session
        
        Returns:
            Number of mismatches found
        """
        releases = await self.pool.fetch(
            """
            SELECT r.release_id, r.release_name, r.spotify_id, r.release_mbid,
                    a.spotify_id AS artist_spotify_id
            FROM releases
            JOIN artists a ON r.primary_artist_id = a.artist_id
            WHERE r.spotify_id IS NOT NULL AND r.on_spotify IS TRUE
            """
        )
        
        print(f"\n🔍 Validating {len(releases)} albums...")
        mismatches = 0
        
        for release in releases:
            # validate spotify id format
            spotify_id = release["spotify_id"]
            db_title = release["release_name"]
            release_id = release["release_id"]
            release_mbid = release["release_mbid"]
            primary_artist_spid = release['artist_spotify_id']
            
            if not isinstance(spotify_id, str) or len(spotify_id) != 22:
                self.log_mismatch(
                    "album",
                    release_id,
                    db_title,
                    "N/A",
                    0.0,
                    "malformed_id"
                )
                mismatches += 1
                continue
            
            # mb check
            if release_mbid:
                try:
                    mb_release = await self.mb_client.get_release(release_mbid, includes=["url-rels"])
                    if mb_release:
                        official_ids = self.mb_client.extract_spotify_id(mb_release.get("url-relation-list", []))
                        if official_ids and spotify_id not in official_ids:
                            self.log_mismatch(
                                "artist", 
                                release_id, 
                                db_title, 
                                f"DB: {spotify_id} | MB Official: {official_ids}", 
                                0.0, 
                                "mb_relationship_mismatch"
                            )
                            mismatches += 1
                            
                            async with self.pool.acquire() as conn:
                                await conn.execute(
                                    "UPDATE releases SET spotify_id = NULL, on_spotify = NULL WHERE release_id = $1", 
                                    release_id
                                )
                            print(f"  → Nulled Spotify ID (MB Relationship Mismatch)")
                            continue
                        
                except Exception as e:
                    print(f"  ⚠️ Skipping MB check for {db_title}: {e}")
                    
            else:
                pass
                        
            # fetch from spotify
            spotify_album = await self.spotify.request(
                session,
                f"/albums/{spotify_id}"
            )
            
            if not spotify_album:
                self.log_mismatch(
                    "album",
                    release_id,
                    db_title,
                    "N/A",
                    0.0,
                    "not_found"
                )
                mismatches += 1
                continue
            
            spotify_name = spotify_album.get("name")
            if not spotify_name:
                self.log_mismatch(
                    "album",
                    release_id,
                    db_title,
                    "N/A",
                    0.0,
                    "missing_name"
                )
                mismatches += 1
                continue
            
            # primary artist check
            sp_artist_id = spotify_album['artists'][0]['id']
            if sp_artist_id != primary_artist_spid:
                self.log_mismatch(
                    "album",
                    release_id,
                    db_title,
                    f"DB: {primary_artist_spid} | Spotify Official: {sp_artist_id}",
                    0.0,
                    "primary_artist_mismatch"
                )
                mismatches += 1
            
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE releasess SET spotify_id = NULL, on_spotify = NULL WHERE release_id = $1", 
                        release_id
                    )
                print(f"  → Nulled Spotify ID (Primary Artist Mismatch)")
                continue
            
            # validate match
            if spotify_name.lower() == db_title.lower():
                continue
            
            is_valid, score = self.matcher.validate_release_match(
                db_title,
                spotify_name
            )
            
            if not is_valid:
                self.log_mismatch(
                    "album",
                    release["release_id"],
                    release["release_name"],
                    spotify_name,
                    score,
                    "mismatch"
                )
                mismatches += 1
                
                # null out very bad matches
                if score < 0.55:
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE releases SET spotify_id = NULL, on_spotify = NULL WHERE release_id = $1",
                            release["release_id"]
                        )
                    print(f"  → Nulled Spotify ID (score too low)")
            
            await asyncio.sleep(SpotifyConfig.REQUEST_DELAY)
        
        return mismatches
    
    async def validate_tracks(self, session: aiohttp.ClientSession) -> int:
        """
        Validate all track matches.
        
        Args:
            session: aiohttp session
        
        Returns:
            Number of mismatches found
        """
        tracks = await self.pool.fetch(
            """
            SELECT t.track_id, t.track_name, t.spotify_id, 
                   r.spotify_id AS release_spotify_id
            FROM tracks t
            JOIN releases r ON t.release_id = r.release_id
            WHERE t.spotify_id IS NOT NULL AND t.on_spotify IS TRUE
            LIMIT 1000
            """
        )
        
        print(f"\n🔍 Validating {len(tracks)} tracks (sample)...")
        mismatches = 0
        
        # batch fetch for efficiency
        spotify_ids = [t["spotify_id"] for t in tracks]
        
        for i in range(0, len(spotify_ids), 50):
            batch = spotify_ids[i:i+50]
            
            result = await self.spotify.request(
                session,
                "/tracks",
                params={"ids": ",".join(batch)}
            )
            
            if not result or "tracks" not in result:
                continue
            
            for j, spotify_track in enumerate(result["tracks"]):
                track = tracks[i + j]
                
                if not spotify_track:
                    self.log_mismatch(
                        "track",
                        track["track_id"],
                        track["track_name"],
                        "N/A",
                        0.0,
                        "not_found"
                    )
                    mismatches += 1
                    continue
                
                # parent album check
                album_spid = spotify_track.get("album", {}).get("id")
                if album_spid != track['release_spotify_id']:
                    self.log_mismatch(
                        "track",
                        track["track_id"],
                        track["track_name"],
                        f"Wrong Album: {album_spid}",
                        0.0,
                        "ALBUM_MISMATCH"
                    )
                    mismatches += 1
                    
                    await self.pool.execute(
                        "UPDATE tracks SET spotify_id = NULL, on_spotify = NULL WHERE track_id = $1",
                        track["track_id"]
                    )
                    continue
                
                spotify_name = spotify_track.get("name")
                
                if not spotify_name:
                    self.log_mismatch(
                        "track",
                        track["track_id"],
                        track["track_name"],
                        "N/A",
                        0.0,
                        "missing_name"
                    )
                    mismatches += 1
                    continue
                
                if track["track_name"].lower() == spotify_name.lower():
                    continue
                
                # validate match
                is_valid, score = self.matcher.validate_track_match(
                    track["track_name"],
                    spotify_name
                )
                
                if not is_valid:
                    self.log_mismatch(
                        "track",
                        track["track_id"],
                        track["track_name"],
                        spotify_name,
                        score,
                        "mismatch"
                    )
                    mismatches += 1

                    if score < 0.70:
                        await self.pool.execute(
                            "UPDATE tracks SET spotify_id = NULL, on_spotify = NULL WHERE track_id = $1",
                            track["track_id"]
                        )   
            
            await asyncio.sleep(SpotifyConfig.REQUEST_DELAY)
        
        return mismatches
    
    def save_report(self) -> None:
        """Save mismatches to CSV file."""
        if not self.mismatches:
            print(f"\n✅ No mismatches found!")
            return
        
        with open(self.output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["entity_type", "entity_id", "db_name", "spotify_name", "score", "error_type"]
            )
            writer.writeheader()
            writer.writerows(self.mismatches)
        
        print(f"\n📄 Saved {len(self.mismatches)} mismatches to {self.output_file}")
    
    async def run(self, session: aiohttp.ClientSession) -> None:
        """Run complete validation process."""
        print("🔍 Starting Spotify validation...")
        
        artist_mismatches = await self.validate_artists(session)
        album_mismatches = await self.validate_albums(session)
        track_mismatches = await self.validate_tracks(session)
        
        total = artist_mismatches + album_mismatches + track_mismatches
        
        print(f"\n{'='*60}")
        print("✅ Validation completed!")
        print(f"  Artist mismatches: {artist_mismatches}")
        print(f"  Album mismatches:  {album_mismatches}")
        print(f"  Track mismatches:  {track_mismatches}")
        print(f"  Total:             {total}")
        print(f"{'='*60}")
        
        self.save_report()


async def main():
    """Entry point for Spotify validation."""
    pool = await asyncpg.create_pool(**DatabaseConfig.as_dict())
    
    try:
        validator = SpotifyValidator(pool)
        async with aiohttp.ClientSession() as session:
            await validator.run(session)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())