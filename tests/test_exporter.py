import unittest
from unittest.mock import MagicMock
from scripts.export_with_existing_clients import DataExporter

class TestDataExporterLogic(unittest.TestCase):
    """Unit tests for offline data processing methods in DataExporter."""
    
    def setUp(self):
        # initialize exporter without triggering directory adjustments or netwwork
        self.exporter = DataExporter(output_dir="./test_parquet_export")
        
    def test_extract_mbids_safely_handles_valid_and_null_payloads(self):
        """Verify MBID extraction parses data correctly and bypasses null structures w/o crashing."""
        sample_listens = [
            {
                "track_metadata": {
                    "mbid_mapping": {
                        "artist_mbids": ["artist-uuid-1111"],
                        "release_mbid": "release-uuid-2222",
                        "recording_mbid": "recording-uuid-3333"
                    },
                    "additional_info": {}
                }
            },
            {
                "track_metadata": {
                    # Simulated dirty data payload that could cause a TypeError
                    "mbid_mapping": {
                        "artist_mbids": None,
                        "release_mbid": None,
                        "recording_mbid": None
                    },
                    "additional_info": {
                        "artist_mbids": ["artist-uuid-fallback"]
                    }
                }
            }
        ]
        
        # Run extraction logic
        extracted = self.exporter.extract_mbids(sample_listens)

        # Asserts
        self.assertIn("artist-uuid-1111", extracted["artist_mbids"])
        self.assertIn("artist-uuid-fallback", extracted["artist_mbids"])
        self.assertIn("release-uuid-2222", extracted["release_mbids"])
        self.assertIn("recording-uuid-3333", extracted["recording_mbids"])
        
        # Verify that null records didn't pollute tracking keys as None entries
        self.assertNotIn(None, extracted["artist_mbids"])
        self.assertNotIn(None, extracted["release_mbids"])

if __name__ == "__main__":
    unittest.main()