import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging

from src.storage import SegmentDatabase

# Set up logging
logger = logging.getLogger(__name__)

class SegmentAnalyzer:
    """Analysis tools for Strava segment data"""
    
    def __init__(self, db: SegmentDatabase):
        """Initialize with a database connection"""
        self.db = db
    
    def get_segment_performance_trends(self, segment_id: int) -> pd.DataFrame:
        """
        Analyze performance trends for a specific segment
        
        Args:
            segment_id: Strava segment ID
            
        Returns:
            DataFrame with segment effort data
        """
        efforts = self.db.get_segment_efforts_by_segment(segment_id)
        
        if not efforts:
            logger.warning(f"No efforts found for segment {segment_id}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(efforts)
        
        # Convert date strings to datetime objects
        df['start_date'] = pd.to_datetime(df['start_date'])
        
        # Sort by date
        df = df.sort_values('start_date')
        
        # Calculate additional metrics
        if 'distance' in df.columns and 'elapsed_time' in df.columns:
            # Calculate pace in minutes per km
            df['pace'] = (df['elapsed_time'] / 60) / (df['distance'] / 1000)
            
            # Calculate speed in km/h
            df['speed_kph'] = (df['distance'] / 1000) / (df['elapsed_time'] / 3600)
            
            # Calculate power-to-weight if available
            if 'average_watts' in df.columns:
                # Use a default weight if not available
                # In a real app, you would get this from user settings
                weight_kg = 70  
                df['power_to_weight'] = df['average_watts'] / weight_kg
        
        # Add relative performance (% from personal best)
        best_time = df['elapsed_time'].min()
        df['pct_from_pb'] = (df['elapsed_time'] - best_time) / best_time * 100
        
        # Calculate rolling averages
        if len(df) >= 3:
            df['rolling_avg_3'] = df['elapsed_time'].rolling(window=3, min_periods=1).mean()
        
        return df
    
    def get_seasonal_comparison(self, segment_id: int) -> Dict[str, pd.DataFrame]:
        """
        Compare segment performance across different seasons
        
        Args:
            segment_id: Strava segment ID
            
        Returns:
            Dictionary with DataFrames for each season
        """
        df = self.get_segment_performance_trends(segment_id)
        
        if df.empty:
            return {}
        
        # Add season information
        df['year'] = df['start_date'].dt.year
        df['month'] = df['start_date'].dt.month
        
        # Define seasons (Northern Hemisphere)
        # Winter: Dec-Feb, Spring: Mar-May, Summer: Jun-Aug, Fall: Sep-Nov
        season_map = {
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Fall', 10: 'Fall', 11: 'Fall'
        }
        df['season'] = df['month'].map(season_map)
        
        # Group by season
        seasons = {}
        for season in df['season'].unique():
            seasons[season] = df[df['season'] == season]
            
        return seasons
    
    def get_weather_adjusted_performance(self, segment_id: int) -> pd.DataFrame:
        """
        Adjust segment performance based on weather conditions
        Note: This is a simplified version. In reality, you would need to fetch weather data
        for each effort date and location.
        
        Args:
            segment_id: Strava segment ID
            
        Returns:
            DataFrame with weather-adjusted metrics
        """
        df = self.get_segment_performance_trends(segment_id)
        
        if df.empty:
            return df
        
        # In a real implementation, you would:
        # 1. Get segment coordinates from the segments table
        # 2. Fetch historical weather data for each effort date and location
        # 3. Apply adjustments based on wind speed/direction, temperature, etc.
        
        # This is a simplified example using random "weather factors"
        # In reality, these would be calculated based on actual weather data
        np.random.seed(42)  # For reproducible results
        df['weather_factor'] = np.random.normal(1.0, 0.05, size=len(df))
        
        # Adjust elapsed time based on weather factor
        df['weather_adjusted_time'] = df['elapsed_time'] / df['weather_factor']
        
        return df
    
    def get_personal_records_by_segment(self, limit: int = 10) -> List[Dict]:
        """
        Get segments with personal records
        
        Args:
            limit: Maximum number of segments to retrieve
            
        Returns:
            List of segments with PR data
        """
        # Get popular segments
        popular_segments = self.db.get_popular_segments(limit)
        
        results = []
        for segment_id, name, count in popular_segments:
            # Get segment details
            segment = self.db.get_segment_by_id(segment_id)
            
            # Get best effort
            best_effort = self.db.get_best_efforts_by_segment(segment_id)
            
            if segment and best_effort:
                best = best_effort[0]
                
                # Calculate pace
                pace_min_per_km = (best['elapsed_time'] / 60) / (segment['distance'] / 1000)
                
                results.append({
                    'segment_id': segment_id,
                    'name': segment['name'],
                    'distance': segment['distance'],
                    'avg_grade': segment['average_grade'],
                    'best_time': best['elapsed_time'],
                    'best_date': best['start_date'],
                    'pace_min_per_km': pace_min_per_km,
                    'effort_count': count
                })
        
        return results
    
    def calculate_segment_progress(self, segment_id: int) -> Dict[str, Any]:
        """
        Calculate progress on a segment over time
        
        Args:
            segment_id: Strava segment ID
            
        Returns:
            Dictionary with progress metrics
        """
        df = self.get_segment_performance_trends(segment_id)
        
        if df.empty:
            return {}
        
        # Get segment details
        segment = self.db.get_segment_by_id(segment_id)
        
        if not segment:
            return {}
        
        # Calculate metrics
        first_effort = df.iloc[0]
        last_effort = df.iloc[-1]
        best_effort = df.loc[df['elapsed_time'].idxmin()]
        
        # Calculate improvement
        time_improvement = first_effort['elapsed_time'] - last_effort['elapsed_time']
        pct_improvement = (time_improvement / first_effort['elapsed_time']) * 100
        
        # Calculate days since first effort
        days_training = (last_effort['start_date'] - first_effort['start_date']).days
        
        # Calculate average improvement rate (seconds per day)
        if days_training > 0:
            improvement_rate = time_improvement / days_training
        else:
            improvement_rate = 0
        
        return {
            'segment_name': segment['name'],
            'first_effort_date': first_effort['start_date'],
            'first_effort_time': first_effort['elapsed_time'],
            'last_effort_date': last_effort['start_date'],
            'last_effort_time': last_effort['elapsed_time'],
            'best_effort_date': best_effort['start_date'],
            'best_effort_time': best_effort['elapsed_time'],
            'time_improvement': time_improvement,
            'pct_improvement': pct_improvement,
            'days_training': days_training,
            'improvement_rate': improvement_rate,
            'effort_count': len(df)
        }
    
    def predict_future_performance(self, segment_id: int, days_ahead: int = 30) -> Dict[str, Any]:
        """
        Predict future performance based on historical trends
        
        Args:
            segment_id: Strava segment ID
            days_ahead: Number of days to predict ahead
            
        Returns:
            Dictionary with prediction metrics
        """
        df = self.get_segment_performance_trends(segment_id)
        
        if df.empty or len(df) < 3:  # Need at least a few data points
            return {}
        
        # Simple linear regression on elapsed time vs. date
        # Convert dates to numeric (days since first effort)
        first_date = df['start_date'].min()
        df['days_since_start'] = (df['start_date'] - first_date).dt.days
        
        # Calculate linear fit
        x = df['days_since_start'].values
        y = df['elapsed_time'].values
        
        # Simple linear regression
        if len(x) > 1:
            slope, intercept = np.polyfit(x, y, 1)
            
            # Predict future performance
            future_date = first_date + timedelta(days=days_ahead)
            future_days = days_ahead
            predicted_time = slope * future_days + intercept
            
            # Get current best time
            best_time = df['elapsed_time'].min()
            
            return {
                'current_best': best_time,
                'predicted_time': max(predicted_time, best_time * 0.9),  # Limit improvement to 10%
                'prediction_date': future_date.strftime('%Y-%m-%d'),
                'improvement_trend': 'Improving' if slope < 0 else 'Declining',
                'data_points': len(df)
            }
        
        return {}

# Usage example
if __name__ == "__main__":
    db = SegmentDatabase()
    analyzer = SegmentAnalyzer(db)
    
    # Get popular segments
    popular_segments = db.get_popular_segments(3)
    
    for segment_id, name, count in popular_segments:
        print(f"\nAnalyzing segment: {name}")
        
        # Get progress
        progress = analyzer.calculate_segment_progress(segment_id)
        if progress:
            print(f"Progress over {progress['days_training']} days: {progress['pct_improvement']:.1f}% improvement")
            print(f"Best time: {progress['best_effort_time']} seconds on {progress['best_effort_date']}")
        
        # Get prediction
        prediction = analyzer.predict_future_performance(segment_id)
        if prediction:
            print(f"Predicted time by {prediction['prediction_date']}: {prediction['predicted_time']:.1f} seconds")
            print(f"Trend: {prediction['improvement_trend']}")
    
    db.close()
