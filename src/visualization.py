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
from datetime import datetime, timedelta
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
        
        if not segment:
            logger.warning(f"Segment {segment_id} not found")
            return None
            
        if not segment.get('coordinate_points'):
            logger.warning(f"No coordinate data found for segment {segment_id}")
            
            # Create a placeholder map with instructions
            m = folium.Map(location=[45.5236, -122.6750], zoom_start=13)  # Default location
            folium.Marker(
                [45.5236, -122.6750],
                popup=folium.Popup(
                    f"<h3>No coordinate data available for {segment['name']}</h3>"
                    f"<p>To show maps, fetch segment details from the Strava API using:</p>"
                    f"<pre>python app.py --fetch-segment-details</pre>",
                    max_width=300
                ),
                icon=folium.Icon(color='red', icon='info-sign')
            ).add_to(m)
            
            if save_path:
                m.save(save_path)
            
            return m
        
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
        
    def create_activity_map(self, activity_id: int, save_path: Optional[str] = None) -> Any:
        """
        Create an interactive map showing all segments from an activity
        
        Args:
            activity_id: Activity ID
            save_path: Path to save the HTML map (optional)
            
        Returns:
            Folium map
        """
        # Get the activity details
        cursor = self.db.conn.execute(
            'SELECT * FROM activities WHERE id = ?',
            (activity_id,)
        )
        
        row = cursor.fetchone()
        activity = dict(row) if row is not None else None
        
        if not activity:
            logger.warning(f"Activity {activity_id} not found")
            return None
        
        # Get segments for this activity
        segments = self.db.get_segments_by_activity(activity_id)
        
        if not segments:
            logger.warning(f"No segments found for activity {activity_id}")
            return None
        
        # Initialize map bounds
        min_lat, max_lat = float('inf'), float('-inf')
        min_lng, max_lng = float('inf'), float('-inf')
        
        # Check if we have any segments with coordinate data
        has_coordinates = False
        for segment in segments:
            segment_id = segment['segment_id']
            full_segment = self.db.get_segment_by_id(segment_id)
            if full_segment and full_segment.get('coordinate_points'):
                has_coordinates = True
                break
                
        if not has_coordinates:
            logger.warning(f"No segments with coordinate data found for activity {activity_id}. "
                          "Maps require coordinate data from the Strava API.")
            # Create a placeholder map with a message
            m = folium.Map(location=[45.5236, -122.6750], zoom_start=13)  # Default location
            folium.Marker(
                [45.5236, -122.6750],
                popup=folium.Popup(
                    f"<h3>No coordinate data available</h3>"
                    f"<p>Activity: {activity['name']}</p>"
                    f"<p>To show maps, fetch segment details from the Strava API using:</p>"
                    f"<pre>python app.py --fetch-segment-details</pre>",
                    max_width=300
                ),
                icon=folium.Icon(color='red', icon='info-sign')
            ).add_to(m)
            
            if save_path:
                m.save(save_path)
            
            return m
        
        # Collect all segment coordinates
        all_segments_coords = []
        
        for segment in segments:
            segment_id = segment['segment_id']
            
            # Get the full segment details
            full_segment = self.db.get_segment_by_id(segment_id)
            
            if not full_segment or not full_segment.get('coordinate_points'):
                logger.warning(f"No coordinate data found for segment {segment_id}")
                continue
            
            # Decode polyline
            try:
                points = polyline.decode(full_segment['coordinate_points'])
                
                # Update bounds
                for point in points:
                    min_lat = min(min_lat, point[0])
                    max_lat = max(max_lat, point[0])
                    min_lng = min(min_lng, point[1])
                    max_lng = max(max_lng, point[1])
                
                all_segments_coords.append({
                    'id': segment_id,
                    'name': segment['segment_name'],
                    'points': points,
                    'time': segment['elapsed_time'],
                    'pr_rank': segment.get('pr_rank'),
                    'distance': segment['segment_distance'],
                    'avg_grade': segment['average_grade'],
                    'max_grade': segment['maximum_grade']
                })
            except Exception as e:
                logger.error(f"Error decoding polyline for segment {segment_id}: {e}")
                continue
        
        if not all_segments_coords:
            logger.warning(f"No valid segment coordinates found for activity {activity_id}")
            return None
        
        # Calculate map center
        center_lat = (min_lat + max_lat) / 2
        center_lng = (min_lng + max_lng) / 2
        
        # Create map
        m = folium.Map(location=[center_lat, center_lng])
        
        # Add segments to map with different colors based on PR rank
        colors = {
            1: 'green',  # PR
            None: 'blue'  # Regular effort
        }
        
        # Add segments to map
        for segment_data in all_segments_coords:
            # Choose color based on PR rank
            color = colors.get(segment_data['pr_rank'], 'orange')  # Default to orange for non-PR, non-None ranks
            
            # Format elapsed time
            elapsed_time_formatted = f"{segment_data['time']//60}:{segment_data['time']%60:02d}"
            
            # Create popup content
            popup_html = f"""
            <h3>{segment_data['name']}</h3>
            <p>Distance: {segment_data['distance']/1000:.2f} km</p>
            <p>Grade: {segment_data['avg_grade']:.1f}% (max {segment_data['max_grade']:.1f}%)</p>
            <p>Time: {elapsed_time_formatted}</p>
            """
            
            if segment_data['pr_rank'] == 1:
                popup_html += "<p><strong style='color:green;'>PR Effort!</strong></p>"
            elif segment_data['pr_rank'] is not None:
                popup_html += f"<p>PR Rank: #{segment_data['pr_rank']}</p>"
            
            # Add polyline for this segment
            folium.PolyLine(
                segment_data['points'],
                color=color,
                weight=5,
                opacity=0.7,
                tooltip=f"{segment_data['name']} ({elapsed_time_formatted})",
                popup=folium.Popup(popup_html, max_width=300)
            ).add_to(m)
            
            # Add start and end markers
            start_coords = segment_data['points'][0]
            end_coords = segment_data['points'][-1]
            
            folium.CircleMarker(
                start_coords,
                radius=5,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.7,
                tooltip=f"Start: {segment_data['name']}"
            ).add_to(m)
            
            folium.CircleMarker(
                end_coords,
                radius=5,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.7,
                tooltip=f"End: {segment_data['name']}"
            ).add_to(m)
        
        # Add a legend
        legend_html = """
        <div style="position: fixed; 
            bottom: 50px; right: 50px; width: 150px; height: 90px; 
            border:2px solid grey; z-index:9999; font-size:14px;
            background-color:white; padding: 10px;
            ">
            <p><span style="color:green; font-weight:bold;">▬</span> PR Effort</p>
            <p><span style="color:orange; font-weight:bold;">▬</span> Top 10 Effort</p>
            <p><span style="color:blue; font-weight:bold;">▬</span> Regular Effort</p>
        </div>
        """
        folium.Element(legend_html).add_to(m)
        
        # Fit bounds to include all segments
        if all_segments_coords:
            m.fit_bounds([[min_lat, min_lng], [max_lat, max_lng]])
        
        # Save if requested
        if save_path:
            m.save(save_path)
            logger.info(f"Saved activity map to {save_path}")
        
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
        try:
            progress_data = self.analyzer.calculate_segment_progress(segment_id)
            # Ensure progress is a dictionary
            if isinstance(progress_data, dict):
                progress = progress_data
            else:
                progress = None
                # For tests or incomplete data
                print(f"Warning: progress data for segment {segment_id} was not a dictionary")
        except Exception as e:
            print(f"Error getting progress data: {e}")
            progress = None
        
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
        
        # Create map and save it
        map_filename = f"segment_{segment_id}_map.html"
        map_path = os.path.join(self.output_dir, map_filename)
        segment_map = self.create_segment_map(segment_id, save_path=map_path)
        has_map = segment_map is not None
        
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
                .map-container {{ margin: 20px 0; height: 500px; }}
                h1, h2, h3 {{ color: #333; }}
                .highlight {{ color: #ff5722; font-weight: bold; }}
                .nav-links {{ margin: 20px 0; }}
                .nav-link {{ padding: 10px; background-color: #f0f0f0; text-decoration: none; color: #333; border-radius: 5px; margin-right: 10px; }}
                .nav-link:hover {{ background-color: #e0e0e0; }}
                iframe {{ border: 1px solid #ddd; border-radius: 5px; width: 100%; height: 100%; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{segment['name']}</h1>
                    <p>Distance: {segment['distance']/1000:.2f} km | Average Grade: {segment['average_grade']:.1f}% | Location: {segment.get('city', 'N/A')}, {segment.get('country', 'N/A')}</p>
                    <p>Elevation: Low {segment.get('elevation_low', 0):.1f}m | High {segment.get('elevation_high', 0):.1f}m | Gain {segment.get('elevation_high', 0) - segment.get('elevation_low', 0):.1f}m</p>
                </div>
                
                <div class="nav-links">
                    <a href="segments_summary.html" class="nav-link">Most Popular Segments</a>
                    <a href="recent_activities.html" class="nav-link">Recent Activities</a>
                </div>
        """
        
        if has_map:
            html += f"""
                <div class="map-container">
                    <h2>Segment Map</h2>
                    <iframe src="{map_filename}"></iframe>
                </div>
            """
            
        # Add elevation profile section
        if segment.get('elevation_high') is not None and segment.get('elevation_low') is not None:
            elevation_gain = segment.get('elevation_high', 0) - segment.get('elevation_low', 0)
            elevation_range = segment.get('elevation_high', 0) - segment.get('elevation_low', 0)
            
            # Create a visual representation of the elevation profile
            profile_width = 100
            profile_height = 50
            
            # Generate a realistic elevation profile
            # Get the average grade and use it to create a more accurate profile
            avg_grade = segment.get('average_grade', 0)
            max_grade = segment.get('maximum_grade')
            if max_grade is None or not isinstance(max_grade, (int, float)):
                max_grade = avg_grade * 2 if avg_grade > 0 else 5
            
            # Create control points for the elevation profile
            # Use float coordinates for more accurate rendering and to satisfy type checkers
            points: List[Tuple[float, float]] = []
            
            # Starting point (use floats to match annotation)
            points.append((0.0, float(profile_height)))  # Start at bottom left
            
            # Create a realistic elevation profile with varied gradient
            import random
            random.seed(segment.get('id', 0))  # Use segment ID as seed for consistent randomness
            
            # Number of control points for the profile
            num_points = 12
            
            # Generate control points for the elevation profile
            last_y = profile_height  # Start at bottom
            
            # Define the basic elevation profile shape based on segment characteristics
            if avg_grade > 0:  # It's a climb
                # Create a more realistic climbing profile with variable grades
                # Use more control points for longer segments
                segment_distance = segment.get('distance', 1000)
                if segment_distance > 2000:  # Longer segments get more detail
                    num_points = 16
                
                # Create a climbing profile with varying steepness
                for i in range(1, num_points):
                    x_pos = profile_width * (i / num_points)
                    
                    # Position in the segment (0 to 1)
                    position = i / num_points
                    
                    # Calculate the approximate elevation at this point based on average grade
                    # but add variability to create a more realistic profile
                    
                    # Base elevation change at this point
                    base_elev_change = position * elevation_gain
                    
                    # Add variability - more variability for steeper segments
                    variability = max(5, abs(avg_grade)) / 100  # Scale variability based on grade
                    
                    # Segments typically have steeper and flatter sections
                    # Create a pattern where some sections are steeper than others
                    if position < 0.3:
                        # Beginning section - often steeper for climbs
                        factor = 1.2 + (random.random() * 0.4 - 0.2) * variability
                    elif position < 0.7:
                        # Middle section - can be variable
                        factor = 0.9 + (random.random() * 0.6 - 0.3) * variability
                    else:
                        # End section - often steeper again near the end
                        factor = 1.1 + (random.random() * 0.4 - 0.2) * variability
                    
                    # Calculate height based on relative position and total elevation gain
                    # Invert Y because in SVG 0 is at top, but elevation 0 is at bottom
                    y_pos = profile_height - (base_elev_change * factor / elevation_gain * profile_height)
                    
                    # Ensure y stays within bounds
                    y_pos = max(0, min(profile_height, y_pos))
                    
                    points.append((float(x_pos), float(y_pos)))
                    last_y = y_pos
            else:  # It's flat or a descent
                # For descents, create a profile that starts higher and ends lower
                for i in range(1, num_points):
                    x_pos = profile_width * (i / num_points)
                    position = i / num_points
                    
                    # For descents, we start higher and end lower
                    # Base position goes from 0 to 1, but we invert for descent (1 to 0)
                    base_elev_change = (1 - position) * abs(elevation_gain)
                    
                    # Add some variability
                    variability = max(3, abs(avg_grade)) / 100
                    factor = 1.0 + (random.random() * 0.4 - 0.2) * variability
                    
                    # Calculate height - higher values are lower on screen
                    y_pos = profile_height - (base_elev_change * factor / abs(elevation_gain) * profile_height)
                    y_pos = max(0, min(profile_height, y_pos))
                    
                    points.append((float(x_pos), float(y_pos)))
                    last_y = y_pos
            
            # End point (top right corner) - should match the elevation_high
            points.append((float(profile_width), 0.0))
            points.append((float(profile_width), float(profile_height)))  # Back to bottom to close the polygon
            
            # Create clip-path polygon string from points
            clip_path_points = " ".join([f"{x}% {y}px" for x, y in points])
            
            html += f"""
                <div class="stat-box" style="width: 100%; margin: 20px 0;">
                    <h2>Elevation Profile</h2>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 10px;">
                        <span><b>Start:</b> {segment.get('elevation_low', 0):.1f}m</span>
                        <span><b>Finish:</b> {segment.get('elevation_high', 0):.1f}m</span>
                    </div>
                    <div style="background: linear-gradient(to right, #8BC34A, #FFC107, #F44336); 
                                height: {profile_height}px; width: {profile_width}%; 
                                clip-path: polygon({clip_path_points});">
                    </div>
                    <div style="margin-top: 10px;">
                        <p><b>Total Elevation Gain:</b> {elevation_gain:.1f}m</p>
                        <p><b>Elevation Range:</b> {elevation_range:.1f}m</p>
                        <p><b>Average Grade:</b> {segment.get('average_grade', 0):.1f}% | <b>Maximum Grade:</b> {segment.get('maximum_grade', 'N/A')}%</p>
                    </div>
                </div>
            """
        
        if progress and isinstance(progress, dict):
            # Format dates to strings if they're datetime objects
            best_effort_date = progress.get('best_effort_date', 'N/A')
            first_effort_date = progress.get('first_effort_date', 'N/A')
            last_effort_date = progress.get('last_effort_date', 'N/A')
            
            if hasattr(best_effort_date, 'strftime'):
                best_effort_date = best_effort_date.strftime('%Y-%m-%d')
            if hasattr(first_effort_date, 'strftime'):
                first_effort_date = first_effort_date.strftime('%Y-%m-%d')
            if hasattr(last_effort_date, 'strftime'):
                last_effort_date = last_effort_date.strftime('%Y-%m-%d')
            
            best_effort_time = progress.get('best_effort_time', 'N/A')
            pct_improvement = progress.get('pct_improvement', 0)
            days_training = progress.get('days_training', 0)
            effort_count = progress.get('effort_count', 0)
            
            html += f"""
                <h2>Performance Summary</h2>
                <div class="stats-container">
                    <div class="stat-box">
                        <h3>Best Effort</h3>
                        <p>{best_effort_time} seconds</p>
                        <p>Date: {best_effort_date}</p>
                    </div>
                    <div class="stat-box">
                        <h3>Improvement</h3>
                        <p class="highlight">{pct_improvement:.1f}%</p>
                        <p>Over {days_training} days</p>
                    </div>
                    <div class="stat-box">
                        <h3>Attempts</h3>
                        <p>{effort_count} efforts</p>
                        <p>First: {first_effort_date}</p>
                        <p>Latest: {last_effort_date}</p>
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
                .nav-links { margin: 20px 0; }
                .nav-link { padding: 10px; background-color: #f0f0f0; text-decoration: none; color: #333; border-radius: 5px; margin-right: 10px; }
                .nav-link:hover { background-color: #e0e0e0; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Your Segment Analysis</h1>
                    <p>Top segments based on number of attempts</p>
                </div>
                
                <div class="nav-links">
                    <a href="segments_summary.html" class="nav-link">Most Popular Segments</a>
                    <a href="recent_activities.html" class="nav-link">Recent Activities</a>
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
        
    def create_recent_activities_dashboard(self, days: int = 30, limit: int = 10) -> str:
        """
        Create a dashboard showing recent activities with segment information
        
        Args:
            days: Number of days to look back
            limit: Maximum number of activities to include
            
        Returns:
            HTML content
        """
        # Get recent activities
        recent_activities = self.db.get_recent_activities(days, limit)
        
        if not recent_activities:
            return "<h1>No recent activity data available</h1>"
        
        # Format date for title
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")
        
        # Create HTML content
        html = f"""
        <html>
        <head>
            <title>Recent Activities</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ background-color: #f4f4f4; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f2f2f2; }}
                tr:hover {{ background-color: #f5f5f5; }}
                .activity-link {{ color: #0066cc; text-decoration: none; }}
                .activity-link:hover {{ text-decoration: underline; }}
                h1, h2 {{ color: #333; }}
                .nav-links {{ margin: 20px 0; }}
                .nav-link {{ padding: 10px; background-color: #f0f0f0; text-decoration: none; color: #333; border-radius: 5px; margin-right: 10px; }}
                .nav-link:hover {{ background-color: #e0e0e0; }}
                .date-highlight {{ color: #0066cc; font-weight: bold; }}
                .segment-count {{ font-weight: bold; color: #ff5722; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Recent Activities</h1>
                    <p>Activities between <span class="date-highlight">{from_date}</span> and <span class="date-highlight">{to_date}</span></p>
                </div>
                
                <div class="nav-links">
                    <a href="segments_summary.html" class="nav-link">Most Popular Segments</a>
                    <a href="recent_activities.html" class="nav-link">Recent Activities</a>
                </div>
                
                <table>
                    <tr>
                        <th>Activity</th>
                        <th>Date</th>
                        <th>Type</th>
                        <th>Distance</th>
                        <th>Segments</th>
                        <th>Actions</th>
                    </tr>
        """
        
        for activity in recent_activities:
            # Create a details page for this activity's segments
            if activity['segment_count'] > 0:
                self.create_activity_segments_dashboard(activity['id'])
                activity_url = f"activity_{activity['id']}.html"
                view_action = f'<a href="{activity_url}" class="activity-link">View Segments</a>'
            else:
                view_action = 'No segments'
            
            # Format the date for display
            try:
                date_obj = datetime.fromisoformat(activity['start_date'].replace('Z', '+00:00'))
                formatted_date = date_obj.strftime("%Y-%m-%d %H:%M")
            except (ValueError, AttributeError):
                formatted_date = activity['start_date']
            
            # Format distance in km
            distance_km = round(activity['distance'] / 1000, 2) if activity['distance'] else 0
            
            html += f"""
                    <tr>
                        <td>{activity['name']}</td>
                        <td>{formatted_date}</td>
                        <td>{activity['type']}</td>
                        <td>{distance_km:.2f} km</td>
                        <td class="segment-count">{activity['segment_count']}</td>
                        <td>{view_action}</td>
                    </tr>
            """
        
        html += """
                </table>
            </div>
        </body>
        </html>
        """
        
        # Save HTML to file
        output_path = os.path.join(self.output_dir, "recent_activities.html")
        with open(output_path, 'w') as f:
            f.write(html)
        
        logger.info(f"Recent activities dashboard saved to {output_path}")
        
        return html
        
    def create_activity_segments_dashboard(self, activity_id: int) -> str:
        """
        Create a dashboard showing segments for a specific activity
        
        Args:
            activity_id: Activity ID
            
        Returns:
            HTML content
        """
        # Get the activity details
        cursor = self.db.conn.execute(
            'SELECT * FROM activities WHERE id = ?',
            (activity_id,)
        )
        
        row = cursor.fetchone()
        activity = dict(row) if row is not None else None
        
        if not activity:
            return f"<h1>Activity {activity_id} not found</h1>"
        
        # Get segments for this activity
        segments = self.db.get_segments_by_activity(activity_id)
        
        if not segments:
            return f"<h1>No segments found for activity {activity['name']}</h1>"
        
        # Format date for display
        try:
            date_obj = datetime.fromisoformat(activity['start_date'].replace('Z', '+00:00'))
            formatted_date = date_obj.strftime("%Y-%m-%d %H:%M")
        except (ValueError, AttributeError):
            formatted_date = activity['start_date']
            
        # Create map and save it
        map_filename = f"activity_{activity_id}_map.html"
        map_path = os.path.join(self.output_dir, map_filename)
        activity_map = self.create_activity_map(activity_id, save_path=map_path)
        has_map = activity_map is not None
        
        # Prepare the map HTML section
        map_html = ""
        if has_map:
            map_html = f"""
                <div class="map-container">
                    <h2>Activity Map with Segments</h2>
                    <p>Color legend: <span style="color:green; font-weight:bold;">Green = PR</span>, 
                    <span style="color:orange; font-weight:bold;">Orange = Top 10</span>, 
                    <span style="color:blue; font-weight:bold;">Blue = Regular</span></p>
                    <iframe src="{map_filename}"></iframe>
                </div>
            """
            
        # Create HTML content
        html = f"""
        <html>
        <head>
            <title>Segments for {activity['name']}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ background-color: #f4f4f4; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                .activity-meta {{ margin: 10px 0; color: #666; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f2f2f2; }}
                tr:hover {{ background-color: #f5f5f5; }}
                .segment-link {{ color: #0066cc; text-decoration: none; }}
                .segment-link:hover {{ text-decoration: underline; }}
                h1, h2 {{ color: #333; }}
                .nav-links {{ margin: 20px 0; }}
                .nav-link {{ padding: 10px; background-color: #f0f0f0; text-decoration: none; color: #333; border-radius: 5px; margin-right: 10px; }}
                .nav-link:hover {{ background-color: #e0e0e0; }}
                .pr-rank {{ font-weight: bold; color: #ff5722; }}
                .pr-rank-1 {{ color: #2e7d32; }}
                .map-container {{ margin: 20px 0; height: 500px; }}
                iframe {{ border: 1px solid #ddd; border-radius: 5px; width: 100%; height: 100%; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Segments for {activity['name']}</h1>
                    <div class="activity-meta">
                        <p>Date: {formatted_date}</p>
                        <p>Type: {activity['type']}</p>
                        <p>Distance: {activity['distance']/1000:.2f} km</p>
                        <p>Time: {activity['moving_time']//60}:{activity['moving_time']%60:02d}</p>
                    </div>
                </div>
                
                <div class="nav-links">
                    <a href="segments_summary.html" class="nav-link">Most Popular Segments</a>
                    <a href="recent_activities.html" class="nav-link">Recent Activities</a>
                </div>
                
                {map_html}
                
                <h2>Segments Summary</h2>
                <table>
                    <tr>
                        <th>Segment</th>
                        <th>Distance</th>
                        <th>Grade</th>
                        <th>Time</th>
                        <th>PR</th>
                        <th>Actions</th>
                    </tr>
        """
        
        for segment in segments:
            segment_id = segment['segment_id']
            segment_url = f"segment_{segment_id}.html"
            
            # Ensure the segment dashboard exists
            # This creates the segment page if it doesn't exist yet
            self.create_segment_dashboard(segment_id)
            
            # Format time as mm:ss
            elapsed_time_formatted = f"{segment['elapsed_time']//60}:{segment['elapsed_time']%60:02d}"
            
            # Format PR rank with special styling
            pr_class = "pr-rank pr-rank-1" if segment.get('pr_rank') == 1 else "pr-rank"
            pr_rank_display = f'<span class="{pr_class}">#{segment["pr_rank"]}</span>' if segment.get('pr_rank') else '-'
            
            html += f"""
                    <tr>
                        <td>{segment['segment_name']}</td>
                        <td>{segment['segment_distance']/1000:.2f} km</td>
                        <td>{segment['average_grade']:.1f}% (max {segment['maximum_grade']:.1f}%)</td>
                        <td>{elapsed_time_formatted}</td>
                        <td>{pr_rank_display}</td>
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
        output_path = os.path.join(self.output_dir, f"activity_{activity_id}.html")
        with open(output_path, 'w') as f:
            f.write(html)
        
        logger.info(f"Activity segments dashboard saved to {output_path}")
        
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
