#!/usr/bin/env python3
"""
This script fetches location data for the top 100 most popular segments from the Strava API.
It updates the segments in the database with coordinate points needed for maps.
"""

import os
import sys
import time
import logging
import argparse
from typing import List, Dict, Tuple

from src.storage import SegmentDatabase
from src.data_retrieval import get_segment_details
from src.auth import authenticate

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'segment_locations.log'))
    ]
)
logger = logging.getLogger('fetch_segment_locations')

def fetch_segment_location_data(limit: int = 100, refresh: bool = False) -> None:
    """
    Fetch location data for popular segments from Strava API
    
    Args:
        limit: Maximum number of segments to process
        refresh: Whether to refresh data for segments that already have coordinate data
    """
    # First authenticate with Strava
    tokens = authenticate()
    if not tokens:
        logger.error("Authentication failed")
        return
    
    # Connect to the database
    db = SegmentDatabase()
    
    try:
        # Get the most popular segments
        logger.info(f"Getting top {limit} most popular segments from the database")
        popular_segments = db.get_popular_segments(limit)
        logger.info(f"Found {len(popular_segments)} popular segments")
        
        if not popular_segments:
            logger.warning("No segments found in the database")
            return
        
        # Process each segment
        success_count = 0
        skipped_count = 0
        error_count = 0
        
        for i, (segment_id, name, count) in enumerate(popular_segments):
            logger.info(f"Processing segment {i+1}/{len(popular_segments)}: {name} (ID: {segment_id})")
            
            # Check if we already have coordinate data for this segment
            existing_segment = db.get_segment_by_id(segment_id)
            
            if existing_segment and existing_segment.get('coordinate_points') and not refresh:
                logger.info(f"Segment {segment_id} already has coordinate data, skipping...")
                skipped_count += 1
                continue
            
            try:
                # Fetch segment details from Strava API
                logger.info(f"Fetching details for segment {segment_id} from Strava API")
                segment_detail = get_segment_details(segment_id)
                
                # Check if the response includes coordinate data
                if not segment_detail.get('map', {}).get('polyline'):
                    logger.warning(f"No coordinate data returned for segment {segment_id}")
                    error_count += 1
                    continue
                
                # Save the segment data to the database
                db.save_segment(segment_detail)
                logger.info(f"Successfully updated segment {segment_id} with coordinate data")
                success_count += 1
                
                # Add a small delay to avoid rate limiting
                time.sleep(1.0)
                
            except Exception as e:
                logger.error(f"Error fetching details for segment {segment_id}: {e}", exc_info=True)
                error_count += 1
                # Continue with the next segment
                continue
        
        logger.info(f"Segment location update complete:")
        logger.info(f"- Successfully updated: {success_count}")
        logger.info(f"- Already had data (skipped): {skipped_count}")
        logger.info(f"- Errors: {error_count}")
        logger.info(f"- Total processed: {success_count + skipped_count + error_count}")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        db.close()

def main():
    parser = argparse.ArgumentParser(description="Fetch location data for popular segments from Strava API")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of segments to process")
    parser.add_argument("--refresh", action="store_true", help="Refresh data for segments that already have coordinate data")
    args = parser.parse_args()
    
    fetch_segment_location_data(args.limit, args.refresh)

if __name__ == "__main__":
    sys.exit(main())
