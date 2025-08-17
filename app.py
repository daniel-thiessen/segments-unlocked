import os
import logging
from dotenv import load_dotenv
import argparse
import time
import webbrowser
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import sys

from src.auth import authenticate
from src.data_retrieval import get_activities, get_segment_efforts, get_segment_details
from src.storage import SegmentDatabase
from src.analysis import SegmentAnalyzer
from src.visualization import SegmentVisualizer
from src.archive_import import ArchiveImporter
from src.timestamp_utils import get_latest_activity_timestamp

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

def fetch_activities(db: SegmentDatabase, limit: int = 50, after_date: Optional[int] = None) -> List[Dict]:
    """
    Fetch activities from Strava and store them
    
    Args:
        db: Database connection
        limit: Maximum number of activities to fetch
        after_date: Only fetch activities after this timestamp
        
    Returns:
        List of fetched activities
    """
    if after_date:
        after_date_str = datetime.fromtimestamp(after_date).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Fetching up to {limit} activities since {after_date_str}...")
    else:
        logger.info(f"Fetching up to {limit} recent activities from Strava...")
    
    activities = get_activities(limit, after_date)
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

def generate_visualizations(db: SegmentDatabase, view_recent: bool = False, recent_days: int = 30) -> None:
    """
    Generate visualizations for segments
    
    Args:
        db: Database connection
        view_recent: Whether to view segments by recent activity
        recent_days: Number of days to look back for recent segments
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
    
    # Create recent segments dashboard if requested
    if view_recent:
        recent_path = os.path.join(os.path.dirname(__file__), 'output', 'recent_segments.html')
        logger.info(f"Creating dashboard for segments active in the last {recent_days} days")
        visualizer.create_recent_segments_dashboard(days=recent_days)
        
        # Open recent segments dashboard in browser
        logger.info(f"Opening dashboard: {recent_path}")
        webbrowser.open(f"file://{recent_path}")
    else:
        # Open regular summary dashboard in browser
        if popular_segments:
            logger.info(f"Opening dashboard: {summary_path}")
            webbrowser.open(f"file://{summary_path}")

def main():
    """
Main application entry point

This application provides several functions:
1. Fetch activities from Strava API (all or only new ones since last pull)
2. Import activities from Strava archive exports (ZIP or directory)
3. Generate visualizations for segment efforts
4. Analyze segment performance over time
"""
    parser = argparse.ArgumentParser(description='Personal Strava segment tracker')
    parser.add_argument('--fetch', action='store_true', help='Fetch new data from Strava')
    parser.add_argument('--fetch-new', action='store_true', 
                        help='Fetch only new activities since the last pull')
    parser.add_argument('--limit', type=int, default=50, help='Number of activities to fetch')
    parser.add_argument('--visualize', action='store_true', help='Generate visualizations')
    parser.add_argument('--recent', action='store_true',
                        help='View segments by recent activity')
    parser.add_argument('--recent-days', type=int, default=30,
                        help='Number of days to look back for recent segments')
    parser.add_argument('--refresh-days', type=int, default=30, 
                        help='Number of days after which segment data should be refreshed')
    parser.add_argument('--import-archive', type=str, metavar='PATH',
                        help='Import activities from a Strava archive export (ZIP file or extracted directory)')
    parser.add_argument('--fetch-segment-details', action='store_true',
                        help='Fetch additional segment details for imported segments from the Strava API')
    
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
        
        if args.import_archive:
            # Import data from Strava archive
            archive_path = args.import_archive
            importer = ArchiveImporter(db)
            
            logger.info(f"Importing data from Strava archive: {archive_path}")
            try:
                fetch_segments = args.fetch_segment_details
                
                if archive_path.lower().endswith('.zip'):
                    activities, efforts, segments = importer.import_from_zip(archive_path, fetch_segments=fetch_segments)
                else:
                    activities, efforts, segments = importer.import_from_directory(archive_path, fetch_segments=fetch_segments)
                    
                logger.info(f"Successfully imported {activities} activities with {efforts} segment efforts across {segments} unique segments")
                
                if args.fetch_segment_details and not fetch_segments:
                    updated = importer.fetch_missing_segment_details()
                    logger.info(f"Updated {updated} segments with additional details from the Strava API")
            except Exception as e:
                logger.error(f"Error importing archive: {e}", exc_info=True)
        
        if args.fetch or args.fetch_new:
            # Determine if we need to fetch only new activities
            after_date = None
            if args.fetch_new:
                after_date = get_latest_activity_timestamp()
                if after_date:
                    after_date_str = datetime.fromtimestamp(after_date).strftime("%Y-%m-%d %H:%M:%S")
                    logger.info(f"Fetching activities after {after_date_str}")
                else:
                    logger.warning("No existing activities found. Fetching all activities.")
            
            # Fetch activities
            activities = fetch_activities(db, args.limit, after_date)
            
            # Fetch segment efforts
            effort_count = fetch_segment_efforts(db, activities, args.refresh_days)
            logger.info(f"Fetched and stored {effort_count} segment efforts")
        
        if args.visualize or (not args.fetch and not args.fetch_new and not args.import_archive):
            # Generate visualizations (default action if no other flags)
            generate_visualizations(db, view_recent=args.recent, recent_days=args.recent_days)
        
        db.close()
        logger.info("Application completed successfully")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
