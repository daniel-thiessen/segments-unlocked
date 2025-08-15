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
    def test_rate_limiting(self, mock_time):
        """Test rate limiting functionality."""
        # Setup
        mock_time.time.return_value = 1000.0

        # Reset global variables to a known state for testing
        import src.data_retrieval
        src.data_retrieval.last_request_time = 0
        src.data_retrieval.request_count = 0

        # Execute - first call
        rate_limit_request()
        
        # Assert - first call shouldn't sleep
        mock_time.sleep.assert_not_called()
        self.assertEqual(src.data_retrieval.request_count, 1)

        # Setup for approaching rate limit
        src.data_retrieval.request_count = 99

        # Execute - approaching limit
        rate_limit_request()

        # Assert - should still not sleep
        mock_time.sleep.assert_not_called()
        self.assertEqual(src.data_retrieval.request_count, 100)

        # Execute - hit limit
        rate_limit_request()

        # Assert - should sleep
        mock_time.sleep.assert_called_once()
        # After sleep, count should be reset to 1
        self.assertEqual(src.data_retrieval.request_count, 1)


if __name__ == '__main__':
    unittest.main()
