"""
Tests for the visualization module.
"""
import unittest
import os
import sys
import tempfile
import pandas as pd
import matplotlib
from unittest.mock import patch, MagicMock
import json
import datetime
from datetime import datetime, timedelta
import io

# Use non-interactive backend for testing
matplotlib.use('Agg')

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.visualization import SegmentVisualizer
from src.analysis import SegmentAnalyzer
from src.storage import SegmentDatabase
from tests.mock_data import (
    MOCK_SEGMENT,
    MOCK_SEGMENT_EFFORTS
)


class MockSegmentDatabase:
    """Mock database for testing visualization."""
    
    def __init__(self):
        """Initialize with mock data."""
        self.mock_segment = MOCK_SEGMENT
        self.mock_efforts = MOCK_SEGMENT_EFFORTS
        self.mock_activities = []
        
    def get_segment_by_id(self, segment_id):
        """Return mock segment data."""
        return self.mock_segment
        
    def get_segment_efforts_by_segment(self, segment_id):
        """Return mock segment efforts."""
        return self.mock_efforts
        
    def get_best_efforts_by_segment(self, segment_id, limit=1):
        """Return mock best effort."""
        return [self.mock_efforts[0]]
        
    def get_popular_segments(self, limit=10):
        """Return mock popular segments."""
        return [(MOCK_SEGMENT['id'], MOCK_SEGMENT['name'], 5)]
        
    def close(self):
        """Mock close method."""
        pass


class MockSegmentAnalyzer:
    """Mock analyzer for testing visualization."""
    
    def __init__(self, db=None):
        """Initialize with mock data."""
        self.db = db
        
        # Create sample dataframe for trend analysis
        dates = [datetime.now() - timedelta(days=i*7) for i in range(5)]
        self.mock_df = pd.DataFrame({
            'start_date': dates,
            'elapsed_time': [180, 175, 170, 165, 160],
            'pace': [4.5, 4.3, 4.2, 4.1, 4.0],
            'speed_kph': [13.3, 13.5, 13.8, 14.0, 14.2],
            'power_to_weight': [3.0, 3.1, 3.2, 3.3, 3.4],
            'pct_from_pb': [12.5, 9.4, 6.3, 3.1, 0.0],
            'rolling_avg_3': [178, 175, 172, 168, 165]
        })
        
        # Create seasonal data
        self.mock_seasonal = {
            'Winter': self.mock_df.iloc[0:2].copy(),
            'Summer': self.mock_df.iloc[2:].copy()
        }
        self.mock_seasonal['Winter']['season'] = 'Winter'
        self.mock_seasonal['Summer']['season'] = 'Summer'
        
        # Create progress data
        self.mock_progress = {
            'segment_name': MOCK_SEGMENT['name'],
            'first_effort_date': dates[-1],
            'first_effort_time': 180,
            'last_effort_date': dates[0],
            'last_effort_time': 160,
            'best_effort_date': dates[0],
            'best_effort_time': 160,
            'time_improvement': 20,
            'pct_improvement': 11.1,
            'days_training': 28,
            'improvement_rate': 0.714,
            'effort_count': 5
        }
        
        # Create prediction data
        self.mock_prediction = {
            'current_best': 160,
            'predicted_time': 155.5,
            'prediction_date': (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'),
            'improvement_trend': 'Improving',
            'data_points': 5
        }
        
    def get_segment_performance_trends(self, segment_id):
        """Return mock performance trend dataframe."""
        return self.mock_df
        
    def get_seasonal_comparison(self, segment_id):
        """Return mock seasonal comparison data."""
        return self.mock_seasonal
        
    def calculate_segment_progress(self, segment_id):
        """Return mock segment progress data."""
        return self.mock_progress
        
    def predict_future_performance(self, segment_id, days_ahead=30):
        """Return mock prediction data."""
        return self.mock_prediction
        
    def get_personal_records_by_segment(self, limit=10):
        """Return mock personal records data."""
        return [{
            'segment_id': MOCK_SEGMENT['id'],
            'name': MOCK_SEGMENT['name'],
            'distance': MOCK_SEGMENT['distance'],
            'avg_grade': MOCK_SEGMENT['average_grade'],
            'best_time': 160,
            'best_date': datetime.now().isoformat(),
            'pace_min_per_km': 4.0,
            'effort_count': 5
        }]


class TestSegmentVisualizer(unittest.TestCase):
    """Test cases for the visualization functionality."""

    def setUp(self):
        """Set up the test environment."""
        # Create a temporary directory for output
        self.temp_dir = tempfile.TemporaryDirectory()
        
        # Create mocks
        self.mock_db = MagicMock(spec=SegmentDatabase)
        self.mock_analyzer = MagicMock(spec=SegmentAnalyzer)
        
        # Configure mocks with our mock data behavior
        self.mock_db.get_segment_by_id.return_value = MOCK_SEGMENT
        self.mock_db.get_segment_efforts_by_segment.return_value = MOCK_SEGMENT_EFFORTS
        self.mock_db.get_best_efforts_by_segment.return_value = [MOCK_SEGMENT_EFFORTS[0]]
        self.mock_db.get_popular_segments.return_value = [(MOCK_SEGMENT['id'], MOCK_SEGMENT['name'], 5)]
        
        # Configure analyzer mock
        dates = [datetime.now() - timedelta(days=i*7) for i in range(5)]
        mock_df = pd.DataFrame({
            'start_date': dates,
            'elapsed_time': [180, 175, 170, 165, 160],
            'pace': [4.5, 4.3, 4.2, 4.1, 4.0],
            'speed_kph': [13.3, 13.5, 13.8, 14.0, 14.2],
            'power_to_weight': [3.0, 3.1, 3.2, 3.3, 3.4],
            'pct_from_pb': [12.5, 9.4, 6.3, 3.1, 0.0],
            'rolling_avg_3': [178, 175, 172, 168, 165]
        })
        self.mock_analyzer.get_segment_performance_trends.return_value = mock_df
        
        mock_seasonal = {
            'Winter': mock_df.iloc[0:2].copy(),
            'Summer': mock_df.iloc[2:].copy()
        }
        self.mock_analyzer.get_seasonal_comparison.return_value = mock_seasonal
        
        self.mock_analyzer.calculate_segment_progress.return_value = {
            'segment_name': MOCK_SEGMENT['name'],
            'first_effort_date': dates[-1],
            'first_effort_time': 180,
            'last_effort_date': dates[0],
            'last_effort_time': 160,
            'best_effort_date': dates[0],
            'best_effort_time': 160,
            'time_improvement': 20,
            'pct_improvement': 11.1,
            'days_training': 28,
            'improvement_rate': 0.714,
            'effort_count': 5
        }
        
        self.mock_analyzer.predict_future_performance.return_value = {
            'current_best': 160,
            'predicted_time': 155.5,
            'prediction_date': (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'),
            'improvement_trend': 'Improving',
            'data_points': 5
        }
        
        self.mock_analyzer.get_personal_records_by_segment.return_value = [{
            'segment_id': MOCK_SEGMENT['id'],
            'name': MOCK_SEGMENT['name'],
            'distance': MOCK_SEGMENT['distance'],
            'avg_grade': MOCK_SEGMENT['average_grade'],
            'best_time': 160,
            'best_date': datetime.now().isoformat(),
            'pace_min_per_km': 4.0,
            'effort_count': 5
        }]
        
        # Create the visualizer with mocks
        self.visualizer = SegmentVisualizer(self.mock_db, self.mock_analyzer)
        self.visualizer.output_dir = self.temp_dir.name
        self.visualizer.output_dir = self.temp_dir.name

    def tearDown(self):
        """Clean up resources."""
        self.temp_dir.cleanup()
        
    def test_plot_segment_times(self):
        """Test creating a segment times plot."""
        fig = self.visualizer.plot_segment_times(MOCK_SEGMENT['id'])
        
        # Check that a figure was created
        self.assertIsNotNone(fig)
        
        # Test saving to a file
        save_path = os.path.join(self.temp_dir.name, 'test_plot.png')
        fig = self.visualizer.plot_segment_times(MOCK_SEGMENT['id'], save_path)
        self.assertTrue(os.path.exists(save_path))
        
    def test_plot_pace_distribution(self):
        """Test creating a pace distribution plot."""
        fig = self.visualizer.plot_pace_distribution(MOCK_SEGMENT['id'])
        self.assertIsNotNone(fig)
        
    def test_plot_performance_by_season(self):
        """Test creating a seasonal performance plot."""
        fig = self.visualizer.plot_performance_by_season(MOCK_SEGMENT['id'])
        self.assertIsNotNone(fig)
        
    def test_create_segment_map(self):
        """Test creating a segment map."""
        # Mock the polyline decode function
        with patch('polyline.decode') as mock_decode:
            mock_decode.return_value = [(45.1, -122.5), (45.2, -122.6)]
            
            # Test map creation
            m = self.visualizer.create_segment_map(MOCK_SEGMENT['id'])
            self.assertIsNotNone(m)
            
            # Test with save path
            save_path = os.path.join(self.temp_dir.name, 'test_map.html')
            self.visualizer.create_segment_map(MOCK_SEGMENT['id'], save_path)
            self.assertTrue(os.path.exists(save_path))
            
    def test_create_segment_dashboard(self):
        """Test creating a segment dashboard."""
        # Create dashboard and check output file
        html = self.visualizer.create_segment_dashboard(MOCK_SEGMENT['id'])
        
        # Check that HTML content was generated
        self.assertIsNotNone(html)
        self.assertIn(MOCK_SEGMENT['name'], html)
        
        # Check that file was created
        dashboard_path = os.path.join(self.temp_dir.name, f"segment_{MOCK_SEGMENT['id']}.html")
        self.assertTrue(os.path.exists(dashboard_path))
        
        # Check content
        with open(dashboard_path, 'r') as f:
            content = f.read()
            self.assertIn(MOCK_SEGMENT['name'], content)
            self.assertIn('Performance Summary', content)
            
    def test_create_segments_summary_dashboard(self):
        """Test creating a summary dashboard."""
        # Create summary dashboard
        html = self.visualizer.create_segments_summary_dashboard()
        
        # Check HTML content
        self.assertIsNotNone(html)
        self.assertIn('Your Segment Analysis', html)
        
        # Check file creation
        summary_path = os.path.join(self.temp_dir.name, 'segments_summary.html')
        self.assertTrue(os.path.exists(summary_path))
        
        # Check content
        with open(summary_path, 'r') as f:
            content = f.read()
            self.assertIn('Your Segment Analysis', content)
            self.assertIn(MOCK_SEGMENT['name'], content)
            
    def test_empty_data_handling(self):
        """Test handling of empty data."""
        # Mock empty dataframe
        with patch.object(self.mock_analyzer, 'get_segment_performance_trends', return_value=pd.DataFrame()):
            fig = self.visualizer.plot_segment_times(MOCK_SEGMENT['id'])
            self.assertIsNotNone(fig)  # Should create a figure with "No data available" message

    def test_error_handling(self):
        """Test error handling in visualization."""
        # Test error in map creation
        with patch('polyline.decode', side_effect=Exception("Polyline decode error")):
            m = self.visualizer.create_segment_map(MOCK_SEGMENT['id'])
            self.assertIsNone(m)  # Should return None on error
            
        # Test error in segment retrieval
        with patch.object(self.mock_db, 'get_segment_by_id', return_value=None):
            html = self.visualizer.create_segment_dashboard(999)
            self.assertIn("Segment not found", html)


if __name__ == '__main__':
    unittest.main()
