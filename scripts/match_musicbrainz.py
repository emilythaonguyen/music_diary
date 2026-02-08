"""
Match local entities to MusicBrainz database.
Handles artists, releases, and tracks with fuzzy matching and duplicate merging.
"""
import asyncio
from typing import Optional, List, Dict

import asyncpg
from tqdm import tqdm

from core.config import DatabaseConfig, ValidationConfig
from services.musicbrainz import MusicBrainzClient
from utils.text import similarity_score, transliterate


# foreign key relationships for merging duplicates
FK_RELATIONSHIPS = {
    "artists": [
        "artist_aliases", "artist_genres", "listens", 
        "release_artists", "spotify_artist_popularity", "releases"
    ],
    "releases": [
        "listens", "release_artists", "release_genres", 
        "spotify_release_popularity", "tracks"
    ],
    "tracks": [
        "listens", "spotify_track_popularity", 
        "track_artists", "track_audio_features"
    ]
}


class MusicBrainzMatcher:
    """Match local entities to MusicBrainz."""
    
    def __init__(self, pool: asyncpg.Pool):
        """
        Initialize matcher.
        
        Args:
            pool: Database connection pool
        """
        self.pool = pool
        self.mb = MusicBrainzClient()
    
    async def merge_entity(
        self,
        entity_type: str,
        canonical_id: int,
        duplicate_id: int
    ) -> bool:
        """
        Merge duplicate entity into canonical one.
        
        Args:
            entity_type: "artists", "releases", or "tracks"
            canonical_id: ID to keep
            duplicate_id: ID to merge and delete
        
        Returns:
            True if successful
        """
        pk_column = f"{entity_type[:-1]}_id"
        
        async with self.pool.acquire() as conn:
            # update all foreign key references
            for ref_table in FK_RELATIONSHIPS.get(entity_type, []):
                try:
                    # handle special case where FK column name differs
                    if entity_type == "artists" and ref_table == "releases":
                        fk_column = "primary_artist_id"
                    else:
                        fk_column = pk_column
                    
                    await conn.execute(
                        f"UPDATE {ref_table} SET {fk_column} = $1 WHERE {fk_column} = $2",
                        canonical_id,
                        duplicate_id
                    )
                except Exception as e:
                    print(f"⚠️ Warning: Could not update {ref_table}.{fk_column}: {e}")
            
            # delete the duplicate
            try:
                await conn.execute(
                    f"DELETE FROM {entity_type} WHERE {pk_column} = $1",
                    duplicate_id
                )
                print(f"🟢 Merged {entity_type[:-1]} {duplicate_id} → {canonical_id}")
                return True
            except Exception as e:
                print(f"❌ Failed to delete duplicate {entity_type[:-1]} {duplicate_id}: {e}")
                return False
    
    async def assign_mbid_with_merge(
        self,
        entity_type: str,
        local_id: int,
        mbid: str
    ) -> int:
        """
        Assign MBID to entity, merging duplicates if MBID already exists.
        
        Args:
            entity_type: "artists", "releases", or "tracks"
            local_id: Local entity ID
            mbid: MusicBrainz ID to assign
        
        Returns:
            Final entity ID after any merging
        """
        pk_column = f"{entity_type[:-1]}_id"
        mbid_column = f"{entity_type[:-1]}_mbid"
        
        async with self.pool.acquire() as conn:
            # check if mbid already exists
            existing_id = await conn.fetchval(
                f"SELECT {pk_column} FROM {entity_type} WHERE {mbid_column} = $1",
                mbid
            )
            
            if existing_id and existing_id != local_id:
                # merge duplicate into existing
                await self.merge_entity(entity_type, existing_id, local_id)
                return existing_id
            else:
                # assign mbid to current entity
                await conn.execute(
                    f"UPDATE {entity_type} SET {mbid_column} = $1, on_mb = TRUE WHERE {pk_column} = $2",
                    mbid,
                    local_id
                )
                return local_id
    
    async def match_artist(
        self,
        artist_id: int,
        artist_name: str,
        spotify_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Match artist to MusicBrainz.
        
        Args:
            artist_id: Local artist ID
            artist_name: Artist name
            spotify_id: Spotify ID if available
        
        Returns:
            MusicBrainz ID if matched
        """
        # search musicbrainz
        search_results = await self.mb.search_artists(artist_name, limit=25)
        
        if not search_results:
            return None
        
        # pass 1: match by spotify id
        if spotify_id:
            for candidate in search_results:
                mb_detail = await self.mb.get_artist(
                    candidate["id"],
                    includes=["url-rels"]
                )
                
                if mb_detail:
                    mb_spotify_id = self.mb.extract_spotify_id(
                        mb_detail.get("url-relation-list", [])
                    )
                    
                    if mb_spotify_id == spotify_id:
                        mbid = candidate["id"]
                        
                        # assign mbid (with merge if needed)
                        final_id = await self.assign_mbid_with_merge(
                            "artists",
                            artist_id,
                            mbid
                        )
                        
                        print(f"🟢 Artist '{artist_name}' → MBID {mbid} (Spotify match)")
                        return mbid
        
        # pass 2: best name similarity match
        best_candidate = max(
            search_results,
            key=lambda c: similarity_score(artist_name, c.get("name", ""), method="fuzzy")
        )
        
        score = similarity_score(
            artist_name,
            best_candidate.get("name", ""),
            method="fuzzy"
        )
        
        if score >= ValidationConfig.ARTIST_THRESHOLD:
            mbid = best_candidate["id"]
            
            # get full details to potentially extract Spotify id
            mb_detail = await self.mb.get_artist(mbid, includes=["url-rels"])
            
            if mb_detail:
                mb_spotify_id = self.mb.extract_spotify_id(
                    mb_detail.get("url-relation-list", [])
                )
                
                # fill missing spotify id
                if not spotify_id and mb_spotify_id:
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE artists SET spotify_id = $1 WHERE artist_id = $2",
                            mb_spotify_id,
                            artist_id
                        )
                
                # assign mbid
                final_id = await self.assign_mbid_with_merge("artists", artist_id, mbid)
                
                print(f"🟢 Artist '{artist_name}' → MBID {mbid} (similarity={score:.2f})")
                return mbid
        
        print(f"🔴 No match for artist '{artist_name}' (best score={score:.2f})")
        return None
    
    async def match_release(
        self,
        release_id: int,
        release_name: str,
        artist_name: str,
        spotify_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Match release to MusicBrainz.
        
        Args:
            release_id: Local release ID
            release_name: Release name
            artist_name: Primary artist name
            spotify_id: Spotify ID if available
        
        Returns:
            MusicBrainz ID if matched
        """
        # search musicbrainz
        search_results = await self.mb.search_releases(
            release_name,
            artist=artist_name,
            limit=25
        )
        
        if not search_results:
            return None
        
        # pass 1: match by spotify id
        if spotify_id:
            for candidate in search_results:
                mb_detail = await self.mb.get_release(
                    candidate["id"],
                    includes=["url-rels", "release-groups"]
                )
                
                if mb_detail:
                    mb_spotify_id = self.mb.extract_spotify_id(
                        mb_detail.get("url-relation-list", [])
                    )
                    
                    if mb_spotify_id == spotify_id:
                        mbid = candidate["id"]
                        
                        # Assign MBID
                        final_id = await self.assign_mbid_with_merge(
                            "releases",
                            release_id,
                            mbid
                        )
                        
                        # update release group mbid
                        rg_mbid = mb_detail.get("release-group", {}).get("id")
                        if rg_mbid:
                            async with self.pool.acquire() as conn:
                                await conn.execute(
                                    "UPDATE releases SET release_group_mbid = $1 WHERE release_id = $2",
                                    rg_mbid,
                                    final_id
                                )
                        
                        print(f"🟢 Release '{release_name}' → MBID {mbid} (Spotify match)")
                        return mbid
        
        # pass 2: best name similarity match
        best_candidate = max(
            search_results,
            key=lambda c: similarity_score(release_name, c.get("title", ""), method="fuzzy")
        )
        
        score = similarity_score(
            release_name,
            best_candidate.get("title", ""),
            method="fuzzy"
        )
        
        if score >= ValidationConfig.RELEASE_SIMILARITY:
            mbid = best_candidate["id"]
            
            # get full details
            mb_detail = await self.mb.get_release(
                mbid,
                includes=["url-rels", "release-groups"]
            )
            
            if mb_detail:
                mb_spotify_id = self.mb.extract_spotify_id(
                    mb_detail.get("url-relation-list", [])
                )
                
                # fill missing spotify id
                if not spotify_id and mb_spotify_id:
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE releases SET spotify_id = $1 WHERE release_id = $2",
                            mb_spotify_id,
                            release_id
                        )
                
                # assign mbid
                final_id = await self.assign_mbid_with_merge("releases", release_id, mbid)
                
                # update release group mbid
                rg_mbid = mb_detail.get("release-group", {}).get("id")
                if rg_mbid:
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE releases SET release_group_mbid = $1 WHERE release_id = $2",
                            rg_mbid,
                            final_id
                        )
                
                print(f"🟢 Release '{release_name}' → MBID {mbid} (similarity={score:.2f})")
                return mbid
        
        print(f"🔴 No match for release '{release_name}' (best score={score:.2f})")
        return None
    
    async def fetch_release_tracks(self, release_mbid: str) -> List[Dict]:
        """
        Fetch all tracks from a MusicBrainz release.
        
        Args:
            release_mbid: MusicBrainz release ID
        
        Returns:
            List of track dictionaries
        """
        release = await self.mb.get_release(release_mbid, includes=["recordings"])
        
        if not release:
            return []
        
        tracks = []
        for medium in release.get("medium-list", []):
            for track in medium.get("track-list", []):
                tracks.append({
                    "recording_mbid": track["recording"]["id"],
                    "position": int(track["position"]),
                    "title": track["recording"]["title"],
                    "title_latin": transliterate(track["recording"]["title"])
                })
        
        return tracks
    
    async def match_track(
        self,
        track_id: int,
        track_name: str,
        track_number: Optional[int],
        release_tracks: List[Dict],
        min_similarity: float = 0.55
    ) -> Optional[str]:
        """
        Match track to MusicBrainz recording.
        
        Args:
            track_id: Local track ID
            track_name: Track name
            track_number: Track position on release
            release_tracks: List of tracks from MB release
            min_similarity: Minimum similarity score
        
        Returns:
            Recording MBID if matched
        """
        if not track_number:
            return None
        
        # filter by position
        position_matches = [
            r for r in release_tracks
            if r["position"] == track_number
        ]
        
        if not position_matches:
            return None
        
        # find best match
        best_score = 0.0
        best_candidate = None
        
        for candidate in position_matches:
            score_original = similarity_score(
                track_name,
                candidate["title"],
                method="fuzzy"
            )
            score_translit = similarity_score(
                track_name,
                candidate["title_latin"],
                method="fuzzy"
            )
            score = max(score_original, score_translit)
            
            if score > best_score:
                best_score = score
                best_candidate = candidate
        
        if best_candidate and best_score >= min_similarity:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE tracks SET recording_mbid = $1, on_mb = TRUE WHERE track_id = $2",
                    best_candidate["recording_mbid"],
                    track_id
                )
            
            print(f"🟢 Track '{track_name}' → '{best_candidate['title']}' (score={best_score:.2f})")
            return best_candidate["recording_mbid"]
        
        return None
    
    async def process_artists(self) -> None:
        """Process all artists missing MBIDs."""
        artists = await self.pool.fetch(
            """
            SELECT artist_id, artist_name, spotify_id 
            FROM artists 
            WHERE artist_mbid IS NULL AND on_mb IS NULL
            """
        )
        
        print(f"\n🎨 Processing {len(artists)} artists...")
        
        for artist in tqdm(artists, desc="Artists"):
            await self.match_artist(
                artist["artist_id"],
                artist["artist_name"],
                artist.get("spotify_id")
            )
            await asyncio.sleep(0.5)
    
    async def process_releases(self) -> None:
        """Process all releases missing MBIDs."""
        releases = await self.pool.fetch(
            """
            SELECT r.release_id, r.release_name, r.spotify_id, a.artist_name
            FROM releases r
            JOIN artists a ON r.primary_artist_id = a.artist_id
            WHERE r.release_mbid IS NULL AND r.on_mb IS NULL
            """
        )
        
        print(f"\n💿 Processing {len(releases)} releases...")
        
        for release in tqdm(releases, desc="Releases"):
            mbid = await self.match_release(
                release["release_id"],
                release["release_name"],
                release["artist_name"],
                release.get("spotify_id")
            )
            
            # Process tracks for this release
            if mbid:
                await self.process_tracks_for_release(release["release_id"], mbid)
            
            await asyncio.sleep(0.5)
    
    async def process_tracks_for_release(
        self,
        release_id: int,
        release_mbid: str
    ) -> None:
        """
        Process all tracks for a specific release.
        
        Args:
            release_id: Local release ID
            release_mbid: MusicBrainz release ID
        """
        tracks = await self.pool.fetch(
            """
            SELECT track_id, track_name, track_number 
            FROM tracks 
            WHERE release_id = $1 AND recording_mbid IS NULL
            """,
            release_id
        )
        
        if not tracks:
            return
        
        release_tracks = await self.fetch_release_tracks(release_mbid)
        
        for track in tracks:
            await self.match_track(
                track["track_id"],
                track["track_name"],
                track.get("track_number"),
                release_tracks
            )
            await asyncio.sleep(0.2)
    
    async def process_orphan_tracks(self) -> None:
        """Process tracks whose releases now have MBIDs but tracks don't."""
        tracks = await self.pool.fetch(
            """
            SELECT t.track_id, t.track_name, t.track_number, r.release_mbid
            FROM tracks t
            JOIN releases r ON t.release_id = r.release_id
            WHERE t.recording_mbid IS NULL
              AND r.release_mbid IS NOT NULL
              AND t.on_mb IS NULL
            """
        )
        
        if not tracks:
            return
        
        print(f"\n🎵 Processing {len(tracks)} orphan tracks...")
        
        # group by release to avoid fetching same release multiple times
        from collections import defaultdict
        tracks_by_release = defaultdict(list)
        
        for track in tracks:
            tracks_by_release[track["release_mbid"]].append(track)
        
        for release_mbid, release_tracks in tqdm(
            tracks_by_release.items(),
            desc="Orphan tracks"
        ):
            mb_tracks = await self.fetch_release_tracks(release_mbid)
            
            for track in release_tracks:
                await self.match_track(
                    track["track_id"],
                    track["track_name"],
                    track.get("track_number"),
                    mb_tracks
                )
                await asyncio.sleep(0.2)
    
    async def run(self) -> None:
        """Run complete MusicBrainz matching process."""
        print("🎵 Starting MusicBrainz matching process...")
        
        await self.process_artists()
        await self.process_releases()
        await self.process_orphan_tracks()
        
        print("\n✅ MusicBrainz matching completed!")


async def main():
    """Entry point for MusicBrainz matching."""
    pool = await asyncpg.create_pool(**DatabaseConfig.as_dict())
    
    try:
        matcher = MusicBrainzMatcher(pool)
        await matcher.run()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
