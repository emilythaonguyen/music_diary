#!/usr/bin/env python3
"""
Music Diary - Command-line interface for managing your music listening data.

Available commands:
    import       Import listens from ListenBrainz
    match-mb     Match entities to MusicBrainz
    match-sp     Match entities to Spotify
    validate     Validate existing matches
    metadata     Fetch metadata from APIs
"""
import sys
import asyncio
from pathlib import Path

# add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def print_usage():
    """Print usage information."""
    print("""
Music Diary CLI

Usage: python cli.py <command> [options]

Commands:
    import              Import listening history from ListenBrainz
    match-musicbrainz   Match local entities to MusicBrainz
    match-spotify       Match local entities to Spotify
    fetch-metadata      Fetch metadata from MusicBrainz and Spotify
    validate-musicbrainz Validate MusicBrainz matches
    validate-spotify    Validate Spotify matches

Examples:
    python cli.py import
    python cli.py match-musicbrainz
    python cli.py match-spotify
    python cli.py fetch-metadata
    python cli.py validate-spotify

For more information on a specific command:
    python cli.py <command> --help
""")


async def run_import():
    """Run ListenBrainz import."""
    from scripts.import_listenbrainz import main
    await main()


async def run_match_musicbrainz():
    """Run MusicBrainz matching."""
    from scripts.match_musicbrainz import main
    await main()


async def run_match_spotify():
    """Run Spotify matching."""
    from scripts.match_spotify import main
    await main()


async def run_fetch_metadata():
    """Fetch metadata from APIs."""
    from scripts.fetch_metadata import main
    await main()


async def run_validate_musicbrainz():
    """Validate MusicBrainz matches."""
    from scripts.validate_musicbrainz import main
    await main()


async def run_validate_spotify():
    """Validate Spotify matches."""
    from scripts.validate_spotify import main
    await main()


def main():
    """Main CLI entry point."""
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    commands = {
        'import': run_import,
        'match-musicbrainz': run_match_musicbrainz,
        'match-mb': run_match_musicbrainz,
        'match-spotify': run_match_spotify,
        'match-sp': run_match_spotify,
        'fetch-metadata': run_fetch_metadata,
        'metadata': run_fetch_metadata,
        'validate-musicbrainz': run_validate_musicbrainz,
        'validate-mb': run_validate_musicbrainz,
        'validate-spotify': run_validate_spotify,
        'validate-sp': run_validate_spotify,
    }
    
    if command in ['--help', '-h', 'help']:
        print_usage()
        sys.exit(0)
    
    if command not in commands:
        print(f"❌ Unknown command: {command}")
        print_usage()
        sys.exit(1)
    
    try:
        asyncio.run(commands[command]())
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()