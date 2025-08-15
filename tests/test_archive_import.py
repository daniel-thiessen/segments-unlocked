import unittest
import os
import json
import tempfile
from unittest.mock import patch, MagicMock

from src.storage import SegmentDatabase
from src.archive_import import ArchiveImporter

class TestArchiveImport(unittest.TestCase):
    """Test the Strava archive import functionality"""
    
    def setUp(self):
        # Create a temporary database
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp()
        self.db = SegmentDatabase(self.temp_db_path)
        self.importer = ArchiveImporter(self.db)
        
        # Sample activity data
        self.sample_activity = {
            "id": 1234567890,
            "name": "Morning Run",
            "type": "Run",
            "start_date": "2023-01-01T08:00:00Z",
            "distance": 10000.0,
            "moving_time": 3600,
            "elapsed_time": 3660,
            "total_elevation_gain": 100.0,
            "average_speed": 2.78,
            "max_speed": 3.5,
            "segment_efforts": [
                {
                    "id": 9876543210,
                    "segment": {
                        "id": 5555555,
                        "name": "Test Segment",
                        "activity_type": "Run",
                        "distance": 1000.0,
                        "average_grade": 2.0,
                        "maximum_grade": 5.0,
                        "elevation_high": 100.0,
                        "elevation_low": 80.0,
                        "start_latlng": [40.0, -70.0],
                        "end_latlng": [40.1, -70.1]
                    },
                    "name": "Test Segment",
                    "elapsed_time": 300,
                    "moving_time": 295,
                    "start_date": "2023-01-01T08:05:00Z",
                    "distance": 1000.0,
                    "average_watts": 250.0,
                    "pr_rank": 1
                }
            ]
        }
    
    def tearDown(self):
        # Close the database and remove the temporary file
        self.db.close()
        os.close(self.temp_db_fd)
        os.unlink(self.temp_db_path)
    
    def test_import_from_directory(self):
        # Create a temporary directory to simulate an extracted archive
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create the activities directory
            activities_dir = os.path.join(temp_dir, "activities")
            os.makedirs(activities_dir, exist_ok=True)
            
            # Create a sample activity file
            activity_path = os.path.join(activities_dir, "activity_1234567890.json")
            with open(activity_path, 'w', encoding='utf-8') as f:
                json.dump(self.sample_activity, f)
            
            # Import from the directory
            activities, efforts, segments = self.importer.import_from_directory(temp_dir)
            
            # Assert the correct counts
            self.assertEqual(activities, 1, "Should import exactly one activity")
            self.assertEqual(efforts, 1, "Should import exactly one segment effort")
            self.assertEqual(segments, 1, "Should import exactly one segment")
            
            # Verify the data was saved correctly
            cursor = self.db.conn.execute("SELECT id, name FROM activities WHERE id = ?", (1234567890,))
            activity = cursor.fetchone()
            self.assertIsNotNone(activity, "Activity should be saved in the database")
            self.assertEqual(activity['name'], "Morning Run", "Activity name should match")
            
            cursor = self.db.conn.execute("SELECT id, name FROM segments WHERE id = ?", (5555555,))
            segment = cursor.fetchone()
            self.assertIsNotNone(segment, "Segment should be saved in the database")
            self.assertEqual(segment['name'], "Test Segment", "Segment name should match")
            
            cursor = self.db.conn.execute("SELECT id, segment_id FROM segment_efforts WHERE id = ?", (9876543210,))
            effort = cursor.fetchone()
            self.assertIsNotNone(effort, "Segment effort should be saved in the database")
            self.assertEqual(effort['segment_id'], 5555555, "Segment ID should match")
    
    @patch('src.archive_import.zipfile.ZipFile')
    @patch('src.archive_import.ArchiveImporter.import_from_directory')
    @patch('src.archive_import.os.path.exists')
    def test_import_from_zip(self, mock_exists, mock_import_dir, mock_zipfile):
        # Mock the file existence check
        mock_exists.return_value = True
        
        # Setup mock for zipfile
        mock_zip_instance = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_instance
        
        # Setup mock for import_from_directory
        mock_import_dir.return_value = (1, 1, 1)
        
        # Call import_from_zip
        result = self.importer.import_from_zip("fake_archive.zip")
        
        # Verify zipfile was called with the correct parameters
        mock_zipfile.assert_called_once()
        mock_zip_instance.extractall.assert_called_once()
        
        # Verify import_from_directory was called
        mock_import_dir.assert_called_once()
        
        # Verify correct result
        self.assertEqual(result, (1, 1, 1))
    
    @patch('src.archive_import.get_segment_details')
    def test_fetch_missing_segment_details(self, mock_get_segment):
        # Setup mock segment return
        mock_segment = {
            "id": 5555555,
            "name": "Test Segment",
            "activity_type": "Run",
            "distance": 1000.0,
            "average_grade": 2.0,
            "maximum_grade": 5.0,
            "elevation_high": 100.0,
            "elevation_low": 80.0,
            "map": {"polyline": "abc123"},
            "city": "Test City",
            "country": "Test Country"
        }
        mock_get_segment.return_value = mock_segment
        
        # Insert a segment with incomplete data
        with self.db.conn:
            self.db.conn.execute(
                '''
                INSERT INTO segments (
                    id, name, activity_type, raw_data
                ) VALUES (?, ?, ?, ?)
                ''', 
                (5555555, "Test Segment", "Run", "{}")
            )
        
        # Call fetch_missing_segment_details
        updated = self.importer.fetch_missing_segment_details()
        
        # Verify get_segment_details was called
        mock_get_segment.assert_called_once_with(5555555)
        
        # Verify the segment was updated
        self.assertEqual(updated, 1, "One segment should be updated")
        
        # Verify the segment data was updated
        cursor = self.db.conn.execute("SELECT id, name, city FROM segments WHERE id = ?", (5555555,))
        segment = cursor.fetchone()
        self.assertEqual(segment['city'], "Test City", "Segment city should be updated")

if __name__ == "__main__":
    unittest.main()
