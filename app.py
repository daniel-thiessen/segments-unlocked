import os
import logging
from dotenv import load_dotenv
import argparse
import time
import webbrowser
from datetime import datetime, timedelta
from typing import List, Dict, Any
import sys

from src.auth import authenticate
from src.data_retrieval import get_activities, get_segment_efforts, get_segment_details
from src.storage import SegmentDatabase
from src.analysis import SegmentAnalyzer
from src.visualization import SegmentVisualizer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'segments_unlocked.log'))
    ]
)
logger = logging.getLogger('segments_unlocked')

# Load environment variables
load_dotenv()

def setup_environment() -> bool:
    """
    Check if the environment is properly set up
    
    Returns:
        True if setup is complete, False otherwise
    """
    # Check for required directories
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    config_dir = os.path.join(os.path.dirname(__file__), 'config')
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(config_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    # Check for .env file
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if not os.path.exists(env_path):
        logger.warning("No .env file found. Creating template...")
        with open(env_path, 'w') as f:
            f.write("# Strava API credentials\n")
            f.write("STRAVA_CLIENT_ID=\n")
            f.write("STRAVA_CLIENT_SECRET=\n")
            f.write("STRAVA_REDIRECT_URI=http://localhost:8000/callback\n")
        logger.info(f"Please add your Strava API credentials to {env_path}")
        return False
    
    # Check for Strava credentials
    if not os.getenv('STRAVA_CLIENT_ID') or not os.getenv('STRAVA_CLIENT_SECRET'):
        logger.error("STRAVA_CLIENT_ID and/or STRAVA_CLIENT_SECRET not set in .env file")
        return False
    
    return True

def fetch_activities(db: SegmentDatabase, limit: int = 50) -> List[Dict]:
    """
    Fetch activities from Strava and store them
    
    Args:
        db: Database connection
        limit: Maximum number of activities to fetch
        
    Returns:
        List of fetched activities
    """
    logger.info(f"Fetching up to {limit} recent activities from Strava...")
    
    activities = get_activities(limit)
    logger.info(f"Retrieved {len(activities)} activities")
    
    for activity in activities:
        db.save_activity(activity)
        
    return activities

def fetch_segment_efforts(db: SegmentDatabase, activities: List[Dict], refresh_threshold_days: int = 30) -> int:
    """
    Fetch segment efforts for activities and store them
    
    Args:
        db: Database connection
        activities: List of activities to process
        refresh_threshold_days: Number of days after which segment data should be refreshed
        
    Returns:
        Number of segment efforts found
    """
    total_efforts = 0
    processed_segments = set()  # Track already processed segments to avoid duplicates
    refresh_threshold = datetime.now() - timedelta(days=refresh_threshold_days)
    
    for activity in activities:
        activity_id = activity['id']
        logger.info(f"Fetching segment efforts for activity {activity['name']}")
        
        # Add small delay between activity requests
        time.sleep(1)
        
        efforts = get_segment_efforts(activity_id)
        logger.info(f"Found {len(efforts)} segment efforts")
        
        for effort in efforts:
            db.save_segment_effort(effort)
            
            # Also save the segment definition (only if not already in the database)
            segment_id = effort['segment']['id']
            if segment_id not in processed_segments:
                processed_segments.add(segment_id)
                # First check if we already have this segment in the database
                existing_segment = db.get_segment_by_id(segment_id)
                
                if existing_segment is None:
                    # Segment doesn't exist, fetch from API
                    try:
                        logger.info(f"Fetching details for new segment {segment_id}")
                        segment_detail = get_segment_details(segment_id)
                        db.save_segment(segment_detail)
                        # Add a small delay between segment detail requests to avoid rate limiting
                        time.sleep(0.5)
                    except Exception as e:
                        logger.warning(f"Could not fetch details for segment {segment_id}: {e}")
                else:
                    # Check if segment data needs to be refreshed (based on fetched_at timestamp)
                    needs_refresh = False
                    if existing_segment.get('fetched_at'):
                        try:
                            fetched_date = datetime.fromisoformat(existing_segment['fetched_at'])
                            if fetched_date < refresh_threshold:
                                needs_refresh = True
                                logger.info(f"Refreshing segment {segment_id} data (last updated: {fetched_date.date()})")
                        except (ValueError, TypeError):
                            # If we can't parse the date, refresh the data
                            needs_refresh = True
                    
                    if needs_refresh:
                        try:
                            segment_detail = get_segment_details(segment_id)
                            db.save_segment(segment_detail)
                            time.sleep(0.5)
                        except Exception as e:
                            logger.warning(f"Could not refresh segment {segment_id}: {e}")
                    else:
                        logger.debug(f"Using cached data for segment {segment_id} ({existing_segment['name']})")
        
        total_efforts += len(efforts)
    
    return total_efforts

def generate_visualizations(db: SegmentDatabase) -> None:
    """
    Generate visualizations for segments
    
    Args:
        db: Database connection
    """
    analyzer = SegmentAnalyzer(db)
    visualizer = SegmentVisualizer(db, analyzer)
    
    # Get popular segments
    popular_segments = db.get_popular_segments(10)
    logger.info(f"Generating visualizations for {len(popular_segments)} popular segments")
    
    for segment_id, name, count in popular_segments:
        logger.info(f"Creating dashboard for segment: {name}")
        visualizer.create_segment_dashboard(segment_id)
    
    # Create summary dashboard
    summary_path = os.path.join(os.path.dirname(__file__), 'output', 'segments_summary.html')
    visualizer.create_segments_summary_dashboard()
    
    # Open dashboard in browser
    if popular_segments:
        logger.info(f"Opening dashboard: {summary_path}")
        webbrowser.open(f"file://{summary_path}")

def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(description='Personal Strava segment tracker')
    parser.add_argument('--fetch', action='store_true', help='Fetch new data from Strava')
    parser.add_argument('--limit', type=int, default=50, help='Number of activities to fetch')
    parser.add_argument('--visualize', action='store_true', help='Generate visualizations')
    parser.add_argument('--refresh-days', type=int, default=30, 
                        help='Number of days after which segment data should be refreshed')
    
    args = parser.parse_args()
    
    # Check environment
    if not setup_environment():
        logger.error("Environment setup incomplete. Please configure your settings.")
        return
    
    try:
        # Authenticate with Strava
        tokens = authenticate()
        if not tokens:
            logger.error("Authentication failed")
            return
            
        # Create database connection
        db = SegmentDatabase()
        
        if args.fetch:
            # Fetch activities
            activities = fetch_activities(db, args.limit)
            
            # Fetch segment efforts
            effort_count = fetch_segment_efforts(db, activities, args.refresh_days)
            logger.info(f"Fetched and stored {effort_count} segment efforts")
        
        if args.visualize or not args.fetch:
            # Generate visualizations (default action if no flags)
            generate_visualizations(db)
        
        db.close()
        logger.info("Application completed successfully")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
