"""
Tests for the data retrieval module.
"""
import unittest
from unittest.mock import patch, MagicMock
import json
import os
import sys
import tempfile

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data_retrieval import (
    get_activities,
    get_activity_details,
    get_segment_efforts,
    get_segment_details,
    make_api_request,
    rate_limit_request
)
from tests.mock_data import (
    MOCK_ACTIVITIES,
    MOCK_ACTIVITY,
    MOCK_SEGMENT_EFFORT,
    MOCK_SEGMENT,
    MOCK_SEGMENT_EFFORTS,
    MOCK_SEGMENT_STREAMS
)


class TestDataRetrieval(unittest.TestCase):
    """Test cases for data retrieval functionality."""

    @patch('src.data_retrieval.get_access_token')
    @patch('src.data_retrieval.requests.get')
    def test_make_api_request(self, mock_get, mock_get_token):
        """Test making an API request."""
        # Setup
        mock_get_token.return_value = "fake_token"
        mock_response = MagicMock()
        mock_response.json.return_value = MOCK_ACTIVITIES
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        # Execute
        result = make_api_request("/athlete/activities", {"per_page": 30})

        # Assert
        mock_get.assert_called_once()
        self.assertEqual(result, MOCK_ACTIVITIES)
        mock_get.assert_called_with(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": "Bearer fake_token"},
            params={"per_page": 30}
        )

    @patch('src.data_retrieval.make_api_request')
    def test_get_activities(self, mock_make_request):
        """Test retrieving activities."""
        # Setup
        mock_make_request.side_effect = [
            MOCK_ACTIVITIES[:1],  # First page
            []  # Second page (empty)
        ]

        # Execute
        result = get_activities(limit=1)

        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['id'], MOCK_ACTIVITIES[0]['id'])
        mock_make_request.assert_called_with(
            "/athlete/activities",
            {"per_page": 30, "page": 2}
        )

    @patch('src.data_retrieval.make_api_request')
    def test_get_activity_details(self, mock_make_request):
        """Test retrieving activity details."""
        # Setup
        mock_make_request.return_value = MOCK_ACTIVITY

        # Execute
        result = get_activity_details(MOCK_ACTIVITY['id'])

        # Assert
        self.assertEqual(result, MOCK_ACTIVITY)
        mock_make_request.assert_called_once_with(
            f"/activities/{MOCK_ACTIVITY['id']}"
        )

    @patch('src.data_retrieval.get_activity_details')
    def test_get_segment_efforts(self, mock_get_activity):
        """Test retrieving segment efforts."""
        # Setup
        activity_with_efforts = {
            **MOCK_ACTIVITY,
            "segment_efforts": MOCK_SEGMENT_EFFORTS
        }
        mock_get_activity.return_value = activity_with_efforts

        # Execute
        result = get_segment_efforts(MOCK_ACTIVITY['id'])

        # Assert
        self.assertEqual(result, MOCK_SEGMENT_EFFORTS)
        mock_get_activity.assert_called_once_with(MOCK_ACTIVITY['id'])

    @patch('src.data_retrieval.make_api_request')
    def test_get_segment_details(self, mock_make_request):
        """Test retrieving segment details."""
        # Setup
        mock_make_request.return_value = MOCK_SEGMENT

        # Execute
        result = get_segment_details(MOCK_SEGMENT['id'])

        # Assert
        self.assertEqual(result, MOCK_SEGMENT)
        mock_make_request.assert_called_once_with(
            f"/segments/{MOCK_SEGMENT['id']}"
        )

    @patch('src.data_retrieval.time')
    @patch('src.data_retrieval.random')
    def test_rate_limiting(self, mock_random, mock_time):
        """Test rate limiting functionality."""
        # Setup
        mock_time.time.return_value = 1000.0
        # Prevent random delays for testing
        mock_random.uniform.return_value = 0

        # Reset global variables to a known state for testing
        import src.data_retrieval
        src.data_retrieval.last_request_time = 0
        src.data_retrieval.request_count = 0
        
        # Track sleep calls
        sleep_calls = []
        original_sleep = mock_time.sleep
        
        def mock_sleep_func(seconds):
            sleep_calls.append(seconds)
            return None  # Don't actually sleep in tests
        
        mock_time.sleep = mock_sleep_func

        # Execute - first call
        rate_limit_request()
        
        # Assert - should have a small random delay sleep
        self.assertEqual(len(sleep_calls), 1)
        self.assertEqual(src.data_retrieval.request_count, 1)

        # Setup for approaching rate limit but still under threshold
        src.data_retrieval.request_count = 85  # Below our buffer (90)
        sleep_calls.clear()
        
        # Execute - should only have small delay
        rate_limit_request()
        
        # Assert - only small delay, count increments
        self.assertEqual(len(sleep_calls), 1)
        self.assertEqual(sleep_calls[0], 0)  # Our mocked random delay
        self.assertEqual(src.data_retrieval.request_count, 86)

        # Setup for hitting rate limit
        src.data_retrieval.request_count = 95  # Above our buffer threshold (90)
        sleep_calls.clear()
        
        # Execute - should have rate limit sleep
        rate_limit_request()
        
        # Assert - should have longer sleep followed by random delay
        self.assertEqual(len(sleep_calls), 2)  # Rate limit sleep + random delay
        self.assertTrue(sleep_calls[0] > 1.0)  # First sleep should be substantial
        self.assertEqual(src.data_retrieval.request_count, 1)  # Counter should reset to 1 after rate limit


if __name__ == '__main__':
    unittest.main()
