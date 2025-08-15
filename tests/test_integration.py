"""
Integration tests for the data pipeline.
"""
import unittest
import os
import sys
import tempfile
from unittest.mock import patch, MagicMock

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data_retrieval import (
    get_activities,
    get_segment_efforts,
    get_segment_details
)
from src.storage import SegmentDatabase
from tests.mock_data import (
    MOCK_ACTIVITIES,
    MOCK_SEGMENT_EFFORTS,
    MOCK_SEGMENT
)


class TestDataPipeline(unittest.TestCase):
    """Test cases for the entire data pipeline."""

    def setUp(self):
        """Set up a temporary database and mock the API calls."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        self.db = SegmentDatabase(db_path=self.db_path)
        
        # Set up patches for API calls
        self.patch_get_activities = patch('src.data_retrieval.make_api_request')
        self.mock_get_activities = self.patch_get_activities.start()
        
        self.patch_get_activity_details = patch('src.data_retrieval.make_api_request')
        self.mock_get_activity_details = self.patch_get_activity_details.start()
        
        self.patch_get_segment_details = patch('src.data_retrieval.make_api_request')
        self.mock_get_segment_details = self.patch_get_segment_details.start()

    def tearDown(self):
        """Clean up resources."""
        self.patch_get_activities.stop()
        self.patch_get_activity_details.stop()
        self.patch_get_segment_details.stop()
        self.db.close()
        self.temp_dir.cleanup()

    @patch('src.data_retrieval.make_api_request')
    def test_fetch_and_store_activities(self, mock_make_request):
        """Test fetching activities and storing them in the database."""
        # Configure mock to return our test data
        mock_make_request.side_effect = [
            MOCK_ACTIVITIES,  # First page of activities
            []  # Second page (empty)
        ]
        
        # Execute the data fetching process
        activities = get_activities(limit=2)
        
        # Store activities in the database
        for activity in activities:
            self.db.save_activity(activity)
        
        # Verify activities were stored
        stored_activities = self.db.get_latest_activities(limit=10)
        self.assertEqual(len(stored_activities), 2)
        
        # Verify activity details
        activity_ids = [a['id'] for a in stored_activities]
        self.assertIn(MOCK_ACTIVITIES[0]['id'], activity_ids)
        self.assertIn(MOCK_ACTIVITIES[1]['id'], activity_ids)

    @patch('src.data_retrieval.make_api_request')
    @patch('src.data_retrieval.get_activity_details')
    def test_fetch_and_store_segment_efforts(self, mock_get_activity, mock_make_request):
        """Test fetching segment efforts and storing them in the database."""
        # Configure mocks
        activity_with_efforts = {
            **MOCK_ACTIVITIES[0],
            "segment_efforts": MOCK_SEGMENT_EFFORTS
        }
        mock_get_activity.return_value = activity_with_efforts
        
        # Save activity first
        self.db.save_activity(MOCK_ACTIVITIES[0])
        
        # Get segment efforts
        efforts = get_segment_efforts(MOCK_ACTIVITIES[0]['id'])
        
        # Store segment efforts
        for effort in efforts:
            segment_id = effort['segment']['id']
            
            # Mock segment details request
            mock_make_request.return_value = MOCK_SEGMENT
            
            # Get and store segment details
            segment = get_segment_details(segment_id)
            self.db.save_segment(segment)
            
            # Store the effort
            self.db.save_segment_effort(effort)
        
        # Verify segment efforts were stored
        stored_efforts = self.db.get_segment_efforts_by_segment(MOCK_SEGMENT['id'])
        self.assertEqual(len(stored_efforts), 1)
        self.assertEqual(stored_efforts[0]['id'], MOCK_SEGMENT_EFFORTS[0]['id'])

    def test_end_to_end_data_flow(self):
        """Test the entire data flow from retrieval to storage."""
        with patch('src.data_retrieval.make_api_request') as mock_request:
            # Configure mock to return different results for different endpoints
            def mock_api_side_effect(*args, **kwargs):
                endpoint = args[0] if args else kwargs.get('endpoint', '')
                
                if '/athlete/activities' in endpoint:
                    return MOCK_ACTIVITIES
                elif '/activities/' in endpoint:
                    return {**MOCK_ACTIVITIES[0], "segment_efforts": MOCK_SEGMENT_EFFORTS}
                elif '/segments/' in endpoint:
                    return MOCK_SEGMENT
                return {}
                
            mock_request.side_effect = mock_api_side_effect
            
            # Step 1: Fetch activities
            activities = get_activities(limit=2)
            
            # Step 2: Store activities
            for activity in activities:
                self.db.save_activity(activity)
            
            # Step 3: For each activity, fetch and store segment efforts
            for activity in activities:
                efforts = get_segment_efforts(activity['id'])
                
                for effort in efforts:
                    segment_id = effort['segment']['id']
                    
                    # Get and store segment details
                    segment = get_segment_details(segment_id)
                    self.db.save_segment(segment)
                    
                    # Store the effort
                    self.db.save_segment_effort(effort)
            
            # Verify everything was stored correctly
            stored_activities = self.db.get_latest_activities(limit=10)
            self.assertEqual(len(stored_activities), 2)
            
            # Verify segments were stored
            segment = self.db.get_segment_by_id(MOCK_SEGMENT['id'])
            self.assertIsNotNone(segment)
            if segment:  # Add a check to satisfy Pylance
                self.assertEqual(segment['name'], MOCK_SEGMENT['name'])
            
            # Verify efforts were stored
            efforts = self.db.get_segment_efforts_by_segment(MOCK_SEGMENT['id'])
            self.assertEqual(len(efforts), len(MOCK_SEGMENT_EFFORTS))


if __name__ == '__main__':
    unittest.main()
