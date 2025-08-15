"""
Tests for the storage module.
"""
import unittest
import os
import sys
import sqlite3
import tempfile
import json
from unittest.mock import patch, MagicMock

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.storage import SegmentDatabase
from tests.mock_data import (
    MOCK_ACTIVITY,
    MOCK_SEGMENT,
    MOCK_SEGMENT_EFFORT
)


class TestStorage(unittest.TestCase):
    """Test cases for storage functionality."""

    def setUp(self):
        """Set up a temporary database for testing."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        self.db = SegmentDatabase(db_path=self.db_path)

    def tearDown(self):
        """Clean up the temporary database."""
        self.db.close()
        self.temp_dir.cleanup()

    def test_database_initialization(self):
        """Test that the database is initialized correctly."""
        # Verify tables exist
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check activities table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='activities'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check segments table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='segments'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check segment_efforts table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='segment_efforts'")
        self.assertIsNotNone(cursor.fetchone())
        
        conn.close()

    def test_save_activity(self):
        """Test saving an activity to the database."""
        # Save the activity
        activity_id = self.db.save_activity(MOCK_ACTIVITY)
        
        # Verify it was saved
        self.assertEqual(activity_id, MOCK_ACTIVITY['id'])
        
        # Retrieve the activity from the database
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM activities WHERE id = ?", (activity_id,))
        row = cursor.fetchone()
        
        # Check key fields
        self.assertEqual(row['id'], MOCK_ACTIVITY['id'])
        self.assertEqual(row['name'], MOCK_ACTIVITY['name'])
        self.assertEqual(row['distance'], MOCK_ACTIVITY['distance'])
        
        # Check that raw_data contains the full activity JSON
        raw_data = json.loads(row['raw_data'])
        self.assertEqual(raw_data['id'], MOCK_ACTIVITY['id'])
        
        conn.close()

    def test_save_segment(self):
        """Test saving a segment to the database."""
        # Save the segment
        segment_id = self.db.save_segment(MOCK_SEGMENT)
        
        # Verify it was saved
        self.assertEqual(segment_id, MOCK_SEGMENT['id'])
        
        # Retrieve the segment from the database
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM segments WHERE id = ?", (segment_id,))
        row = cursor.fetchone()
        
        # Check key fields
        self.assertEqual(row['id'], MOCK_SEGMENT['id'])
        self.assertEqual(row['name'], MOCK_SEGMENT['name'])
        self.assertEqual(row['distance'], MOCK_SEGMENT['distance'])
        
        # Check that coordinate_points contains the polyline
        self.assertEqual(row['coordinate_points'], MOCK_SEGMENT['map']['polyline'])
        
        conn.close()

    def test_save_segment_effort(self):
        """Test saving a segment effort to the database."""
        # First save the segment (referenced by the effort)
        self.db.save_segment(MOCK_SEGMENT_EFFORT['segment'])
        
        # Save the segment effort
        effort_id = self.db.save_segment_effort(MOCK_SEGMENT_EFFORT)
        
        # Verify it was saved
        self.assertEqual(effort_id, MOCK_SEGMENT_EFFORT['id'])
        
        # Retrieve the segment effort from the database
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM segment_efforts WHERE id = ?", (effort_id,))
        row = cursor.fetchone()
        
        # Check key fields
        self.assertEqual(row['id'], MOCK_SEGMENT_EFFORT['id'])
        self.assertEqual(row['elapsed_time'], MOCK_SEGMENT_EFFORT['elapsed_time'])
        self.assertEqual(row['segment_id'], MOCK_SEGMENT_EFFORT['segment']['id'])
        
        conn.close()

    def test_get_latest_activities(self):
        """Test retrieving the most recent activities."""
        # Save two activities
        activity1 = {**MOCK_ACTIVITY, 'start_date': '2023-05-01T08:00:00Z'}
        activity2 = {**MOCK_ACTIVITY, 'id': 12345678987654322, 'start_date': '2023-05-02T08:00:00Z'}
        
        self.db.save_activity(activity1)
        self.db.save_activity(activity2)
        
        # Retrieve latest activities
        activities = self.db.get_latest_activities(limit=2)
        
        # Check that they're returned in the correct order (most recent first)
        self.assertEqual(len(activities), 2)
        self.assertEqual(activities[0]['id'], activity2['id'])
        self.assertEqual(activities[1]['id'], activity1['id'])

    def test_get_segment_efforts_by_segment(self):
        """Test retrieving all efforts for a specific segment."""
        # Save necessary data
        activity_id = self.db.save_activity(MOCK_ACTIVITY)
        segment_id = self.db.save_segment(MOCK_SEGMENT)
        
        # Create two efforts for the same segment
        effort1 = {**MOCK_SEGMENT_EFFORT, 'id': 1001, 'activity_id': activity_id, 'segment_id': segment_id}
        effort2 = {**MOCK_SEGMENT_EFFORT, 'id': 1002, 'activity_id': activity_id, 'segment_id': segment_id}
        
        self.db.save_segment_effort(effort1)
        self.db.save_segment_effort(effort2)
        
        # Retrieve efforts for the segment
        efforts = self.db.get_segment_efforts_by_segment(segment_id)
        
        # Check that both efforts are returned
        self.assertEqual(len(efforts), 2)
        self.assertTrue(any(e['id'] == effort1['id'] for e in efforts))
        self.assertTrue(any(e['id'] == effort2['id'] for e in efforts))

    def test_get_best_efforts_by_segment(self):
        """Test retrieving the best efforts for a segment."""
        # Save necessary data
        activity_id = self.db.save_activity(MOCK_ACTIVITY)
        segment_id = self.db.save_segment(MOCK_SEGMENT)
        
        # Create efforts with different times
        effort1 = {**MOCK_SEGMENT_EFFORT, 'id': 1001, 'activity_id': activity_id, 'segment_id': segment_id, 'elapsed_time': 180}
        effort2 = {**MOCK_SEGMENT_EFFORT, 'id': 1002, 'activity_id': activity_id, 'segment_id': segment_id, 'elapsed_time': 150}
        
        self.db.save_segment_effort(effort1)
        self.db.save_segment_effort(effort2)
        
        # Get best effort
        best_efforts = self.db.get_best_efforts_by_segment(segment_id, limit=1)
        
        # Check that the faster effort is returned
        self.assertEqual(len(best_efforts), 1)
        self.assertEqual(best_efforts[0]['id'], effort2['id'])
        self.assertEqual(best_efforts[0]['elapsed_time'], 150)


if __name__ == '__main__':
    unittest.main()
