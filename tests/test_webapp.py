"""
Tests for the web app functionality.
"""
import unittest
import os
import sys
import tempfile
from unittest.mock import patch, MagicMock
import json
from datetime import datetime, timedelta

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import app
from src.storage import SegmentDatabase
from src.auth import authenticate
from tests.mock_data import (
    MOCK_ACTIVITIES,
    MOCK_SEGMENT_EFFORTS,
    MOCK_SEGMENT
)


class TestWebApp(unittest.TestCase):
    """Test cases for the web app functionality."""

    def setUp(self):
        """Set up the test environment."""
        # Create a temporary directory for testing
        self.temp_dir = tempfile.TemporaryDirectory()
        
        # Setup patchers
        self.patcher_authenticate = patch('app.authenticate')
        self.patcher_get_activities = patch('app.get_activities')
        self.patcher_get_segment_efforts = patch('app.get_segment_efforts')
        self.patcher_get_segment_details = patch('app.get_segment_details')
        self.patcher_db = patch('app.SegmentDatabase')
        self.patcher_analyzer = patch('app.SegmentAnalyzer')
        self.patcher_visualizer = patch('app.SegmentVisualizer')
        self.patcher_webbrowser = patch('app.webbrowser')
        
        # Start patchers
        self.mock_authenticate = self.patcher_authenticate.start()
        self.mock_get_activities = self.patcher_get_activities.start()
        self.mock_get_segment_efforts = self.patcher_get_segment_efforts.start()
        self.mock_get_segment_details = self.patcher_get_segment_details.start()
        self.mock_db = self.patcher_db.start()
        self.mock_analyzer = self.patcher_analyzer.start()
        self.mock_visualizer = self.patcher_visualizer.start()
        self.mock_webbrowser = self.patcher_webbrowser.start()
        
        # Setup return values
        self.mock_authenticate.return_value = {"access_token": "test_token"}
        self.mock_get_activities.return_value = MOCK_ACTIVITIES
        self.mock_get_segment_efforts.return_value = MOCK_SEGMENT_EFFORTS
        self.mock_get_segment_details.return_value = MOCK_SEGMENT
        self.mock_db_instance = MagicMock()
        self.mock_db.return_value = self.mock_db_instance
        self.mock_db_instance.get_popular_segments.return_value = [
            (MOCK_SEGMENT['id'], MOCK_SEGMENT['name'], 5)
        ]
        
    def tearDown(self):
        """Clean up resources."""
        # Stop all patchers
        self.patcher_authenticate.stop()
        self.patcher_get_activities.stop()
        self.patcher_get_segment_efforts.stop()
        self.patcher_get_segment_details.stop()
        self.patcher_db.stop()
        self.patcher_analyzer.stop()
        self.patcher_visualizer.stop()
        self.patcher_webbrowser.stop()
        
        # Clean up temporary directory
        self.temp_dir.cleanup()
        
    @patch('app.os.getenv')
    def test_setup_environment(self, mock_getenv):
        """Test environment setup."""
        # Mock environment variables
        mock_getenv.side_effect = lambda k: {"STRAVA_CLIENT_ID": "test_id", "STRAVA_CLIENT_SECRET": "test_secret"}.get(k)
        
        # Test setup with valid env
        result = app.setup_environment()
        self.assertTrue(result)
        
        # Check directory creation
        data_dir = os.path.join(os.path.dirname(app.__file__), 'data')
        config_dir = os.path.join(os.path.dirname(app.__file__), 'config')
        output_dir = os.path.join(os.path.dirname(app.__file__), 'output')
        
        self.assertTrue(os.path.exists(data_dir))
        self.assertTrue(os.path.exists(config_dir))
        self.assertTrue(os.path.exists(output_dir))
        
        # Test with missing credentials
        mock_getenv.side_effect = lambda k: None
        result = app.setup_environment()
        self.assertFalse(result)
        
    def test_fetch_activities(self):
        """Test fetching activities."""
        # Call function
        result = app.fetch_activities(self.mock_db_instance, limit=10)
        
        # Check results
        self.assertEqual(result, MOCK_ACTIVITIES)
        self.mock_get_activities.assert_called_once_with(10, None)
        self.mock_db_instance.save_activity.assert_called()
        
    def test_fetch_segment_efforts(self):
        """Test fetching segment efforts."""
        # Call function
        result = app.fetch_segment_efforts(self.mock_db_instance, MOCK_ACTIVITIES)
        
        # Check results
        self.assertEqual(result, len(MOCK_SEGMENT_EFFORTS) * len(MOCK_ACTIVITIES))
        self.mock_get_segment_efforts.assert_called()
        self.mock_db_instance.save_segment_effort.assert_called()
        
    def test_generate_visualizations(self):
        """Test generating visualizations."""
        # Call function
        app.generate_visualizations(self.mock_db_instance)
        
        # Check that visualizations were created
        self.mock_analyzer.assert_called_once_with(self.mock_db_instance)
        self.mock_visualizer.assert_called_once()
        mock_visualizer_instance = self.mock_visualizer.return_value
        mock_visualizer_instance.create_segment_dashboard.assert_called()
        mock_visualizer_instance.create_segments_summary_dashboard.assert_called_once()
        self.mock_webbrowser.open.assert_called_once()
        
    @patch('app.argparse.ArgumentParser.parse_args')
    @patch('app.get_latest_activity_timestamp')
    def test_main_fetch_mode(self, mock_timestamp, mock_parse_args):
        """Test main function with fetch mode."""
        # Mock command line arguments
        mock_parse_args.return_value = MagicMock(
            fetch=True,
            limit=5,
            visualize=False,
            refresh_days=30
        )
        
        # Mock timestamp to match the expected value in the test failure (1755347427)
        mock_timestamp.return_value = 1755347427
        
        # Call main function
        result = app.main()
        
        # Check results
        self.assertEqual(result, 0)  # Should return success
        self.mock_authenticate.assert_called_once()
        self.mock_get_activities.assert_called_once_with(5, 1755347427)
        self.mock_get_segment_efforts.assert_called()
        self.mock_db_instance.close.assert_called_once()
        
    @patch('app.argparse.ArgumentParser.parse_args')
    def test_main_visualize_mode(self, mock_parse_args):
        """Test main function with visualize mode."""
        # Mock command line arguments
        mock_parse_args.return_value = MagicMock(
            fetch=False,
            limit=5,
            visualize=True,
            refresh_days=30
        )
        
        # Call main function
        result = app.main()
        
        # Check results
        self.assertEqual(result, 0)  # Should return success
        self.mock_authenticate.assert_called_once()
        self.mock_analyzer.assert_called_once()
        self.mock_visualizer.assert_called_once()
        self.mock_db_instance.close.assert_called_once()
        
    @patch('app.argparse.ArgumentParser.parse_args')
    def test_main_authentication_failure(self, mock_parse_args):
        """Test main function with authentication failure."""
        # Mock authentication failure
        self.mock_authenticate.return_value = None
        
        # Mock command line arguments
        mock_parse_args.return_value = MagicMock(
            fetch=True,
            limit=5,
            visualize=False,
            refresh_days=30
        )
        
        # Call main function
        result = app.main()
        
        # Check early return without further processing
        self.assertIsNone(result)
        self.mock_authenticate.assert_called_once()
        self.mock_get_activities.assert_not_called()
        self.mock_get_segment_efforts.assert_not_called()
        
    @patch('app.argparse.ArgumentParser.parse_args')
    @patch('app.setup_environment')
    def test_main_environment_setup_failure(self, mock_setup, mock_parse_args):
        """Test main function with environment setup failure."""
        # Mock environment setup failure
        mock_setup.return_value = False
        
        # Mock command line arguments
        mock_parse_args.return_value = MagicMock(
            fetch=True,
            limit=5,
            visualize=False,
            refresh_days=30
        )
        
        # Call main function
        result = app.main()
        
        # Check early return without further processing
        self.assertIsNone(result)
        mock_setup.assert_called_once()
        self.mock_authenticate.assert_not_called()
        self.mock_get_activities.assert_not_called()
        
    @patch('app.argparse.ArgumentParser.parse_args')
    def test_main_exception_handling(self, mock_parse_args):
        """Test main function exception handling."""
        # Mock error in activity fetching
        self.mock_get_activities.side_effect = Exception("API Error")
        
        # Mock command line arguments
        mock_parse_args.return_value = MagicMock(
            fetch=True,
            limit=5,
            visualize=False,
            refresh_days=30
        )
        
        # Call main function
        result = app.main()
        
        # Check error handling
        self.assertEqual(result, 1)  # Should return error code
        self.mock_authenticate.assert_called_once()
        self.mock_get_activities.assert_called_once()


if __name__ == '__main__':
    unittest.main()
