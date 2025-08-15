import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
import os
import folium
from folium.plugins import HeatMap
import polyline
import io
import base64
import logging
from typing import Any, Optional, Union, Dict, List, Tuple, TypeVar, cast

# Define a type for folium.Map for better type checking
MapType = TypeVar('MapType')

# Handle IPython imports without type checking issues
# We're using a pragmatic approach here to avoid complex typing issues
import sys
if 'IPython' in sys.modules:
    # Only import if IPython is already loaded
    from IPython.display import display, HTML  # type: ignore
else:
    # Create minimal stubs if IPython is not available
    def display(*args: Any, **kwargs: Any) -> None:  # type: ignore
        pass
    
    class HTML:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.data = args[0] if args else ""

from src.analysis import SegmentAnalyzer
from src.storage import SegmentDatabase

# Set up logging
logger = logging.getLogger(__name__)

class SegmentVisualizer:
    """Visualization tools for Strava segment data"""
    
    def __init__(self, db: SegmentDatabase, analyzer: SegmentAnalyzer):
        """Initialize with a database connection and analyzer"""
        self.db = db
        self.analyzer = analyzer
        
        # Create output directory
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')
        os.makedirs(self.output_dir, exist_ok=True)
    
    def plot_segment_times(self, segment_id: int, save_path: Optional[str] = None):
        """
        Create a plot of segment times over time
        
        Args:
            segment_id: Strava segment ID
            save_path: Path to save the figure (optional)
            
        Returns:
            Matplotlib figure
        """
        # Get segment data
        df = self.analyzer.get_segment_performance_trends(segment_id)
        
        if df.empty:
            logger.warning(f"No efforts found for segment {segment_id}")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "No data available", ha='center', va='center', fontsize=14)
            return fig
        
        # Get segment details
        segment = self.db.get_segment_by_id(segment_id)
        segment_name = segment['name'] if segment else f"Segment {segment_id}"
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Plot elapsed time
        ax.plot(df['start_date'], df['elapsed_time'], 'o-', label='Elapsed Time')
        
        # Plot rolling average if available
        if 'rolling_avg_3' in df.columns:
            ax.plot(df['start_date'], df['rolling_avg_3'], 'r--', label='3-Effort Rolling Avg')
        
        # Add best time line
        best_time = df['elapsed_time'].min()
        ax.axhline(best_time, color='green', linestyle='--', label=f'Best Time: {best_time}s')
        
        # Format x-axis for dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45)
        
        # Add labels and title
        ax.set_xlabel('Date')
        ax.set_ylabel('Time (seconds)')
        ax.set_title(f'Performance on {segment_name} Over Time')
        
        # Add grid and legend
        ax.grid(True, alpha=0.3)
        ax.legend()
        
        plt.tight_layout()
        
        # Save if requested
        if save_path:
            plt.savefig(save_path)
            logger.info(f"Saved plot to {save_path}")
            
        return fig
    
    def plot_pace_distribution(self, segment_id: int, save_path: Optional[str] = None):
        """
        Create a histogram of pace distribution
        
        Args:
            segment_id: Strava segment ID
            save_path: Path to save the figure (optional)
            
        Returns:
            Matplotlib figure
        """
        # Get segment data
        df = self.analyzer.get_segment_performance_trends(segment_id)
        
        if df.empty or 'pace' not in df.columns:
            logger.warning(f"No pace data found for segment {segment_id}")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "No pace data available", ha='center', va='center', fontsize=14)
            return fig
        
        # Get segment details
        segment = self.db.get_segment_by_id(segment_id)
        segment_name = segment['name'] if segment else f"Segment {segment_id}"
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Plot histogram
        ax.hist(df['pace'], bins=10, alpha=0.7, color='skyblue', edgecolor='black')
        
        # Add average line
        avg_pace = df['pace'].mean()
        ax.axvline(avg_pace, color='red', linestyle='--', 
                  label=f'Average: {avg_pace:.2f} min/km')
        
        # Add best pace line
        best_pace = df['pace'].min()
        ax.axvline(best_pace, color='green', linestyle='--', 
                  label=f'Best: {best_pace:.2f} min/km')
        
        # Add labels and title
        ax.set_xlabel('Pace (min/km)')
        ax.set_ylabel('Frequency')
        ax.set_title(f'Pace Distribution for {segment_name}')
        
        # Add grid and legend
        ax.grid(True, alpha=0.3)
        ax.legend()
        
        plt.tight_layout()
        
        # Save if requested
        if save_path:
            plt.savefig(save_path)
            logger.info(f"Saved plot to {save_path}")
            
        return fig
    
    def plot_performance_by_season(self, segment_id: int, save_path: Optional[str] = None):
        """
        Create a box plot of performance by season
        
        Args:
            segment_id: Strava segment ID
            save_path: Path to save the figure (optional)
            
        Returns:
            Matplotlib figure
        """
        # Get seasonal data
        seasonal_data = self.analyzer.get_seasonal_comparison(segment_id)
        
        if not seasonal_data:
            logger.warning(f"No seasonal data found for segment {segment_id}")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "No seasonal data available", ha='center', va='center', fontsize=14)
            return fig
        
        # Get segment details
        segment = self.db.get_segment_by_id(segment_id)
        segment_name = segment['name'] if segment else f"Segment {segment_id}"
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Prepare data for boxplot
        data = []
        labels = []
        for season in ['Winter', 'Spring', 'Summer', 'Fall']:
            if season in seasonal_data and not seasonal_data[season].empty:
                data.append(seasonal_data[season]['elapsed_time'])
                labels.append(f"{season} (n={len(seasonal_data[season])})")
        
        # Create boxplot
        if data:
            ax.boxplot(data)
            
            # Add labels and title
            ax.set_ylabel('Time (seconds)')
            ax.set_title(f'Seasonal Performance Comparison for {segment_name}')
            
            # Add grid
            ax.grid(True, alpha=0.3)
            
            plt.tight_layout()
        else:
            ax.text(0.5, 0.5, "Not enough seasonal data available", ha='center', va='center', fontsize=14)
        
        # Save if requested
        if save_path:
            plt.savefig(save_path)
            logger.info(f"Saved plot to {save_path}")
            
        return fig
    
    def create_segment_map(self, segment_id: int, save_path: Optional[str] = None) -> Any:
        """
        Create an interactive map of the segment
        
        Args:
            segment_id: Strava segment ID
            save_path: Path to save the HTML map (optional)
            
        Returns:
            Folium map
        """
        # Get segment details
        segment = self.db.get_segment_by_id(segment_id)
        
        if not segment or not segment['coordinate_points']:
            logger.warning(f"No coordinate data found for segment {segment_id}")
            return None
        
        # Decode polyline
        try:
            points = polyline.decode(segment['coordinate_points'])
        except Exception as e:
            logger.error(f"Error decoding polyline: {e}")
            return cast(Any, None)
        
        if not points:
            logger.warning(f"No valid points in polyline for segment {segment_id}")
            return cast(Any, None)
        
        # Extract start and end coordinates
        start_coords = points[0]
        end_coords = points[-1]
        
        # Calculate map center
        center_lat = sum(point[0] for point in points) / len(points)
        center_lng = sum(point[1] for point in points) / len(points)
        
        # Create map
        m = folium.Map(location=[center_lat, center_lng], zoom_start=14)
        
        # Add segment polyline
        folium.PolyLine(
            points,
            color='blue',
            weight=5,
            opacity=0.7,
            tooltip=segment['name']
        ).add_to(m)
        
        # Add start marker
        folium.Marker(
            start_coords,
            popup='Start',
            icon=folium.Icon(color='green', icon='play')
        ).add_to(m)
        
        # Add end marker
        folium.Marker(
            end_coords,
            popup='End',
            icon=folium.Icon(color='red', icon='flag')
        ).add_to(m)
        
        # Add segment metadata
        distance_km = segment['distance'] / 1000
        elevation_gain = segment['elevation_high'] - segment['elevation_low']
        
        html = f"""
        <h3>{segment['name']}</h3>
        <p>Distance: {distance_km:.2f} km</p>
        <p>Avg Grade: {segment['average_grade']:.1f}%</p>
        <p>Elevation Gain: {elevation_gain:.1f} m</p>
        """
        
        folium.Popup(html).add_to(m)
        
        # Save if requested
        if save_path:
            m.save(save_path)
            logger.info(f"Saved map to {save_path}")
            
        return m
    
    def create_segment_dashboard(self, segment_id: int) -> str:
        """
        Create a simple HTML dashboard for a segment
        
        Args:
            segment_id: Strava segment ID
            
        Returns:
            HTML content
        """
        # Get segment details
        segment = self.db.get_segment_by_id(segment_id)
        
        if not segment:
            return "<h1>Segment not found</h1>"
        
        # Get progress data
        progress = self.analyzer.calculate_segment_progress(segment_id)
        
        # Get prediction
        prediction = self.analyzer.predict_future_performance(segment_id)
        
        # Create performance plot
        fig_perf = self.plot_segment_times(segment_id)
        
        # Save figure to base64 string
        buf = io.BytesIO()
        fig_perf.savefig(buf, format='png')
        buf.seek(0)
        plot_data = base64.b64encode(buf.read()).decode('utf-8')
        plt.close(fig_perf)
        
        # Create pace plot
        fig_pace = self.plot_pace_distribution(segment_id)
        buf = io.BytesIO()
        fig_pace.savefig(buf, format='png')
        buf.seek(0)
        pace_plot_data = base64.b64encode(buf.read()).decode('utf-8')
        plt.close(fig_pace)
        
        # Create HTML content
        html = f"""
        <html>
        <head>
            <title>{segment['name']} Analysis</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ background-color: #f4f4f4; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                .stats-container {{ display: flex; flex-wrap: wrap; }}
                .stat-box {{ background-color: #f9f9f9; border: 1px solid #ddd; border-radius: 5px; padding: 15px; margin: 10px; flex: 1; }}
                .plot-container {{ margin: 20px 0; }}
                h1, h2, h3 {{ color: #333; }}
                .highlight {{ color: #ff5722; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{segment['name']}</h1>
                    <p>Distance: {segment['distance']/1000:.2f} km | Average Grade: {segment['average_grade']:.1f}% | Location: {segment.get('city', 'N/A')}, {segment.get('country', 'N/A')}</p>
                </div>
        """
        
        if progress:
            html += f"""
                <h2>Performance Summary</h2>
                <div class="stats-container">
                    <div class="stat-box">
                        <h3>Best Effort</h3>
                        <p>{progress['best_effort_time']} seconds</p>
                        <p>Date: {progress['best_effort_date']}</p>
                    </div>
                    <div class="stat-box">
                        <h3>Improvement</h3>
                        <p class="highlight">{progress['pct_improvement']:.1f}%</p>
                        <p>Over {progress['days_training']} days</p>
                    </div>
                    <div class="stat-box">
                        <h3>Attempts</h3>
                        <p>{progress['effort_count']} efforts</p>
                        <p>First: {progress['first_effort_date']}</p>
                        <p>Latest: {progress['last_effort_date']}</p>
                    </div>
            """
            
            if prediction:
                html += f"""
                    <div class="stat-box">
                        <h3>Prediction</h3>
                        <p>Estimated future time: <span class="highlight">{prediction['predicted_time']:.1f} seconds</span></p>
                        <p>By: {prediction['prediction_date']}</p>
                        <p>Trend: {prediction['improvement_trend']}</p>
                    </div>
                """
                
            html += "</div>"  # Close stats-container
        
        html += f"""
                <div class="plot-container">
                    <h2>Performance Over Time</h2>
                    <img src="data:image/png;base64,{plot_data}" width="100%">
                </div>
                
                <div class="plot-container">
                    <h2>Pace Distribution</h2>
                    <img src="data:image/png;base64,{pace_plot_data}" width="100%">
                </div>
            </div>
        </body>
        </html>
        """
        
        # Save HTML to file
        output_path = os.path.join(self.output_dir, f"segment_{segment_id}.html")
        with open(output_path, 'w') as f:
            f.write(html)
        
        logger.info(f"Dashboard saved to {output_path}")
        
        return html
    
    def create_segments_summary_dashboard(self, limit: int = 10) -> str:
        """
        Create a summary dashboard of top segments
        
        Args:
            limit: Maximum number of segments to include
            
        Returns:
            HTML content
        """
        # Get personal records
        records = self.analyzer.get_personal_records_by_segment(limit)
        
        if not records:
            return "<h1>No segment data available</h1>"
        
        # Create HTML content
        html = """
        <html>
        <head>
            <title>Segments Analysis Summary</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 0; padding: 20px; }
                .container { max-width: 1200px; margin: 0 auto; }
                .header { background-color: #f4f4f4; padding: 20px; border-radius: 5px; margin-bottom: 20px; }
                table { width: 100%; border-collapse: collapse; margin: 20px 0; }
                th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
                th { background-color: #f2f2f2; }
                tr:hover { background-color: #f5f5f5; }
                .segment-link { color: #0066cc; text-decoration: none; }
                .segment-link:hover { text-decoration: underline; }
                h1, h2 { color: #333; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Your Segment Analysis</h1>
                    <p>Top segments based on number of attempts</p>
                </div>
                
                <table>
                    <tr>
                        <th>Segment</th>
                        <th>Distance</th>
                        <th>Grade</th>
                        <th>Best Time</th>
                        <th>Pace</th>
                        <th>Attempts</th>
                        <th>Actions</th>
                    </tr>
        """
        
        for record in records:
            segment_url = f"segment_{record['segment_id']}.html"
            
            html += f"""
                    <tr>
                        <td>{record['name']}</td>
                        <td>{record['distance']/1000:.2f} km</td>
                        <td>{record['avg_grade']:.1f}%</td>
                        <td>{record['best_time']} sec</td>
                        <td>{record['pace_min_per_km']:.2f} min/km</td>
                        <td>{record['effort_count']}</td>
                        <td><a href="{segment_url}" class="segment-link">View Analysis</a></td>
                    </tr>
            """
        
        html += """
                </table>
            </div>
        </body>
        </html>
        """
        
        # Save HTML to file
        output_path = os.path.join(self.output_dir, "segments_summary.html")
        with open(output_path, 'w') as f:
            f.write(html)
        
        logger.info(f"Summary dashboard saved to {output_path}")
        
        return html

# Usage example
if __name__ == "__main__":
    db = SegmentDatabase()
    analyzer = SegmentAnalyzer(db)
    visualizer = SegmentVisualizer(db, analyzer)
    
    # Get popular segments
    popular_segments = db.get_popular_segments(3)
    
    for segment_id, name, count in popular_segments:
        print(f"\nCreating visualizations for segment: {name}")
        
        # Create dashboard
        visualizer.create_segment_dashboard(segment_id)
    
    # Create summary dashboard
    visualizer.create_segments_summary_dashboard()
    
    db.close()
