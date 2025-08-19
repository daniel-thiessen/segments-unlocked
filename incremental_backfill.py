#!/usr/bin/env python3
"""
Incremental backfill script for Strava segment efforts and segment details.
This script efficiently fetches segment efforts and segment details from Strava API
while respecting rate limits.
"""

import os
import time
import argparse
import sqlite3
import logging
from typing import Dict, List, Set, Tuple, Optional, Any, Union
from datetime import datetime, timedelta
from collections import defaultdict

# Third-party imports
try:
    import stravalib
    from stravalib.client import Client
    from stravalib.model import SegmentEffort, Segment
except ImportError:
    print("Error: stravalib not installed. Run 'pip install stravalib'.")
    exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Strava API Rate Limits
# 100 requests every 15 minutes, 1000 daily
# https://developers.strava.com/docs/rate-limits/
STRAVA_15MIN_LIMIT = 100
STRAVA_DAILY_LIMIT = 1000
RATE_LIMIT_BUFFER = 0.9  # Use 90% of limit to be safe

class RateLimiter:
    """Track API call counts and enforce rate limits"""
    
    def __init__(self, window_size: int = 15 * 60, max_calls: int = 100):
        self.window_size = window_size  # in seconds
        self.max_calls = max_calls
        self.calls: List[datetime] = []
        self.daily_calls = 0
        self.daily_reset = datetime.now()
    
    def wait_if_needed(self) -> None:
        """Wait if we're approaching rate limits"""
        now = datetime.now()
        
        # Reset daily counter if it's a new day
        if now.date() > self.daily_reset.date():
            logger.info("Resetting daily API call counter")
            self.daily_calls = 0
            self.daily_reset = now
        
        # Check daily limit
        if self.daily_calls >= STRAVA_DAILY_LIMIT * RATE_LIMIT_BUFFER:
            seconds_until_midnight = (datetime.combine(now.date() + timedelta(days=1), 
                                                    datetime.min.time()) - now).seconds
            wait_time = seconds_until_midnight + 5  # Add 5 seconds buffer
            logger.warning(f"Daily rate limit reached. Waiting until midnight ({wait_time} seconds)")
            time.sleep(wait_time)
            self.daily_calls = 0
            self.daily_reset = datetime.now()
            return
        
        # Remove calls outside the current window
        self.calls = [t for t in self.calls if (now - t).total_seconds() < self.window_size]
        
        # If we're approaching the limit, wait until we have room
        if len(self.calls) >= self.max_calls * RATE_LIMIT_BUFFER:
            oldest_call = self.calls[0]
            seconds_to_wait = self.window_size - (now - oldest_call).total_seconds()
            if seconds_to_wait > 0:
                logger.info(f"Approaching rate limit. Waiting for {seconds_to_wait:.2f} seconds")
                time.sleep(seconds_to_wait + 1)  # Add 1 second buffer
                self.calls = self.calls[1:]  # Remove the oldest call
    
    def add_call(self) -> None:
        """Record that we made an API call"""
        self.calls.append(datetime.now())
        self.daily_calls += 1

def safe_duration_to_seconds(duration_obj: Any) -> Optional[int]:
    """
    Safely extract seconds from a duration object, handling different stravalib versions.
    
    Args:
        duration_obj: A duration object from stravalib, could be various implementations
        
    Returns:
        Total seconds as int, or None if conversion fails
    """
    if duration_obj is None:
        return None
        
    try:
        # Try the timedelta interface with total_seconds
        if hasattr(duration_obj, 'total_seconds'):
            return int(duration_obj.total_seconds())
        # Try direct seconds attribute
        elif hasattr(duration_obj, 'seconds'):
            return int(duration_obj.seconds)
        # Try converting to int directly
        else:
            return int(duration_obj)
    except (AttributeError, ValueError, TypeError) as e:
        logger.warning(f"Could not convert duration to seconds: {e}")
        return None


class StravaDatabase:
    """Handle database operations for Strava data"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
    
    def close(self) -> None:
        """Close the database connection"""
        if self.conn:
            self.conn.close()
    
    def get_activities_needing_segment_efforts(self, limit: int = 50) -> List[Dict]:
        """Get activities that need segment efforts"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT a.id, a.start_date
            FROM activities a
            LEFT JOIN (
                SELECT activity_id, COUNT(*) as effort_count
                FROM segment_efforts
                GROUP BY activity_id
            ) e ON a.id = e.activity_id
            WHERE e.effort_count IS NULL OR e.effort_count = 0
            ORDER BY a.start_date DESC
            LIMIT ?
        """, (limit,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def get_unknown_segment_ids(self, limit: int = 100) -> List[int]:
        """Get segment IDs that need detailed information"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT e.segment_id
            FROM segment_efforts e
            LEFT JOIN segments s ON e.segment_id = s.id
            WHERE s.id IS NULL
            LIMIT ?
        """, (limit,))
        
        return [row[0] for row in cursor.fetchall()]
    
    def store_segment_efforts(self, efforts: List[SegmentEffort]) -> None:
        """Store segment efforts in the database"""
        cursor = self.conn.cursor()
        
        for effort in efforts:
            # Skip None efforts or those with missing IDs
            if effort is None or not hasattr(effort, 'id') or effort.id is None:
                logger.warning("Skipping segment effort with no ID")
                continue
                
            # Check if the effort already exists
            cursor.execute("SELECT id FROM segment_efforts WHERE id = ?", (effort.id,))
            if cursor.fetchone():
                continue
            
            # Make sure activity and segment exist
            if (not hasattr(effort, 'activity') or effort.activity is None or 
                not hasattr(effort.activity, 'id')):
                logger.warning(f"Skipping effort {effort.id}: Missing activity information")
                continue
                
            if (not hasattr(effort, 'segment') or effort.segment is None or 
                not hasattr(effort.segment, 'id')):
                logger.warning(f"Skipping effort {effort.id}: Missing segment information")
                continue
                
            # Insert the effort
            cursor.execute("""
                INSERT OR REPLACE INTO segment_efforts (
                    id, activity_id, segment_id, name, elapsed_time, 
                    moving_time, start_date, distance, 
                    average_watts, device_watts, average_heartrate, 
                    max_heartrate, pr_rank, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                effort.id, effort.activity.id, effort.segment.id, 
                effort.name if hasattr(effort, 'name') else f"Effort {effort.id}",
                safe_duration_to_seconds(effort.elapsed_time),
                safe_duration_to_seconds(effort.moving_time),
                effort.start_date.isoformat() if hasattr(effort, 'start_date') and effort.start_date else None,
                float(effort.distance) if hasattr(effort, 'distance') and effort.distance else None,
                float(effort.average_watts) if hasattr(effort, 'average_watts') and effort.average_watts else None,
                1 if hasattr(effort, 'device_watts') and effort.device_watts else 0,  # Store as INTEGER
                float(effort.average_heartrate) if hasattr(effort, 'average_heartrate') and effort.average_heartrate else None,
                float(effort.max_heartrate) if hasattr(effort, 'max_heartrate') and effort.max_heartrate else None,
                effort.pr_rank if hasattr(effort, 'pr_rank') else None,
                str(effort) if effort else None  # Store the raw effort data as string
            ))
        
        self.conn.commit()
        logger.info(f"Stored {len(efforts)} segment efforts in database")
    
    def store_segments(self, segments: List[Segment]) -> None:
        """Store segments in the database"""
        cursor = self.conn.cursor()
        
        for segment in segments:
            # Skip None segments or those with missing IDs
            if segment is None or not hasattr(segment, 'id') or segment.id is None:
                logger.warning("Skipping segment with no ID")
                continue
                
            # Extract start_latlng and end_latlng from segment
            start_latlng = None
            end_latlng = None
            if hasattr(segment, 'start_latlng') and segment.start_latlng:
                start_latlng = str(segment.start_latlng)
            if hasattr(segment, 'end_latlng') and segment.end_latlng:
                end_latlng = str(segment.end_latlng)
                
            # Insert the segment using the actual schema
            cursor.execute("""
                INSERT OR REPLACE INTO segments (
                    id, name, activity_type, distance, average_grade,
                    maximum_grade, elevation_high, elevation_low,
                    start_latlng, end_latlng, climb_category, city, state, 
                    country, private, starred, raw_data, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                segment.id, 
                segment.name if hasattr(segment, 'name') else f"Segment {segment.id}",
                str(segment.activity_type) if hasattr(segment, 'activity_type') and segment.activity_type else None,
                float(segment.distance) if hasattr(segment, 'distance') and segment.distance else None,
                float(segment.average_grade) if hasattr(segment, 'average_grade') and segment.average_grade else None,
                float(segment.maximum_grade) if hasattr(segment, 'maximum_grade') and segment.maximum_grade else None,
                float(segment.elevation_high) if hasattr(segment, 'elevation_high') and segment.elevation_high else None,
                float(segment.elevation_low) if hasattr(segment, 'elevation_low') and segment.elevation_low else None,
                start_latlng, end_latlng,
                segment.climb_category if hasattr(segment, 'climb_category') else None,
                segment.city if hasattr(segment, 'city') else None, 
                segment.state if hasattr(segment, 'state') else None, 
                segment.country if hasattr(segment, 'country') else None,
                1 if hasattr(segment, 'private') and segment.private else 0,
                1 if hasattr(segment, 'starred') and segment.starred else 0,
                str(segment),  # Store raw data
                datetime.now().isoformat()  # Store the current time
            ))
        
        self.conn.commit()
        logger.info(f"Stored {len(segments)} segments in database")
    
    def mark_activity_processed(self, activity_id: int) -> None:
        """Mark an activity as having its segment efforts processed"""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE activities 
            SET segment_efforts_processed = 1
            WHERE id = ?
        """, (activity_id,))
        self.conn.commit()

class StravaBackfill:
    """Handle the incremental backfill process"""
    
    def __init__(self, access_token: Optional[str] = None, db_path: str = 'data/segments.db', 
                 client_id: Optional[int] = None, client_secret: Optional[str] = None, 
                 refresh_token: Optional[str] = None):
        """Initialize with either direct access token or OAuth credentials"""
        self.client = Client()
        self.db = StravaDatabase(db_path)
        self.rate_limiter = RateLimiter(window_size=15 * 60, max_calls=STRAVA_15MIN_LIMIT)
        
        # Store OAuth parameters if provided
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        
        if access_token:
            # Use direct access token approach
            self.client.access_token = access_token
        elif client_id and client_secret and refresh_token:
            # Use OAuth approach
            self._refresh_access_token()
        else:
            raise ValueError("Either access_token or (client_id, client_secret, refresh_token) must be provided")
    
    def _refresh_access_token(self):
        """Refresh the Strava access token using the refresh token"""
        logger.info("Refreshing Strava access token")
        try:
            # Check if we have all required OAuth parameters
            if not self.client_id or not self.client_secret or not self.refresh_token:
                logger.error("Missing required OAuth parameters for token refresh")
                return False
            
            # Safe conversion of client_id to int
            client_id_int = 0
            if isinstance(self.client_id, int):
                client_id_int = self.client_id
            else:
                try:
                    client_id_int = int(str(self.client_id))
                except (ValueError, TypeError):
                    logger.error(f"Invalid client_id: {self.client_id}, must be convertible to int")
                    return False
                
            token_response = self.client.refresh_access_token(
                client_id=client_id_int,  # Already ensured to be an int
                client_secret=str(self.client_secret),  # Ensure client_secret is a string
                refresh_token=str(self.refresh_token)   # Ensure refresh_token is a string
            )
            # Update the refresh token in case it changed
            self.refresh_token = token_response['refresh_token']
            logger.info("Successfully refreshed access token")
            return True
        except Exception as e:
            logger.error(f"Error refreshing access token: {e}")
            return False
    
    def close(self) -> None:
        """Close resources"""
        self.db.close()
    
    def backfill_segment_efforts(self, max_activities: int = 10) -> int:
        """Fetch segment efforts for activities that need them"""
        activities = self.db.get_activities_needing_segment_efforts(max_activities)
        
        if not activities:
            logger.info("No activities found that need segment efforts")
            return 0
        
        logger.info(f"Found {len(activities)} activities that need segment efforts")
        processed_count = 0
        
        for activity in activities:
            if not activity or 'id' not in activity:
                logger.warning("Skipping activity with missing ID")
                continue
                
            start_date = activity.get('start_date', 'unknown date')
            logger.info(f"Processing activity {activity['id']} from {start_date}")
            
            try:
                # Wait if we're approaching rate limits
                self.rate_limiter.wait_if_needed()
                
                # Fetch segment efforts
                activity_data = self.client.get_activity(activity['id'])
                
                # Record the API call
                self.rate_limiter.add_call()
                
                # Handle potential None or missing segment_efforts
                if not activity_data or not hasattr(activity_data, 'segment_efforts'):
                    logger.warning(f"Activity {activity['id']} has no segment_efforts attribute")
                    # Mark as processed to avoid repeated failures
                    self.db.mark_activity_processed(activity['id'])
                    continue
                
                # Convert to list and handle None case
                efforts_list = list(activity_data.segment_efforts) if activity_data.segment_efforts is not None else []
                
                # Store segment efforts in database
                if efforts_list:
                    self.db.store_segment_efforts(efforts_list)
                
                # Mark activity as processed
                self.db.mark_activity_processed(activity['id'])
                
                processed_count += 1
                logger.info(f"Processed activity {activity['id']} with {len(efforts_list)} segment efforts")
                
            except Exception as e:
                logger.error(f"Error processing activity {activity['id']}: {str(e)}")
        
        return processed_count
    
    def backfill_segment_details(self, batch_size: int = 10) -> int:
        """Fetch detailed information for segments"""
        segment_ids = self.db.get_unknown_segment_ids(batch_size * 2)  # Fetch extra in case of errors
        
        if not segment_ids:
            logger.info("No segments found that need details")
            return 0
        
        logger.info(f"Found {len(segment_ids)} segments that need details")
        processed_count = 0
        segments = []
        
        for i, segment_id in enumerate(segment_ids[:batch_size]):
            if segment_id is None:
                logger.warning("Skipping segment with None ID")
                continue
                
            try:
                # Wait if we're approaching rate limits
                self.rate_limiter.wait_if_needed()
                
                # Fetch segment details
                segment = self.client.get_segment(segment_id)
                
                # Skip None segments
                if segment is None:
                    logger.warning(f"No data returned for segment {segment_id}")
                    continue
                    
                # Record the API call
                self.rate_limiter.add_call()
                
                segments.append(segment)
                processed_count += 1
                
                logger.info(f"Fetched details for segment {segment_id} ({i+1}/{min(batch_size, len(segment_ids))})")
                
            except Exception as e:
                logger.error(f"Error fetching segment {segment_id}: {str(e)}")
        
        # Store segments in database
        if segments:
            self.db.store_segments(segments)
        
        return processed_count

def load_env(file_path='.env'):
    """Load environment variables from .env file"""
    if not os.path.exists(file_path):
        logger.error(f".env file not found at {file_path}")
        return {}
        
    env_vars = {}
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            key, value = line.split('=', 1)
            env_vars[key.strip()] = value.strip()
            
    return env_vars

def get_refresh_token(db_path):
    """Get the latest refresh token from the database or tokens file"""
    # First try to get it from a tokens file if it exists
    if os.path.exists('tokens.json'):
        try:
            with open('tokens.json', 'r') as f:
                import json
                tokens = json.load(f)
                return tokens.get('refresh_token')
        except:
            pass
    
    # If not found, try to get it from the database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM tokens WHERE name = 'refresh_token'")
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return row[0]
    except:
        pass
        
    return None

def main():
    parser = argparse.ArgumentParser(description='Incremental backfill for Strava segment efforts and details')
    parser.add_argument('--activities', type=int, default=5,
                        help='Maximum number of activities to process per run')
    parser.add_argument('--segments', type=int, default=10,
                        help='Maximum number of segments to process per run')
    parser.add_argument('--mode', choices=['both', 'efforts', 'segments'], default='both',
                        help='Backfill mode: segment efforts, segment details, or both')
    parser.add_argument('--db', type=str, default='data/segments.db',
                        help='Path to the SQLite database')
    parser.add_argument('--env', type=str, default='.env',
                        help='Path to the .env file with Strava credentials')
    
    args = parser.parse_args()
    
    # Load environment variables from .env file
    env_vars = load_env(args.env)
    
    # Get Strava credentials
    client_id = env_vars.get('STRAVA_CLIENT_ID')
    client_secret = env_vars.get('STRAVA_CLIENT_SECRET')
    
    # Try to get access token directly first
    access_token = os.environ.get('STRAVA_ACCESS_TOKEN')
    
    if access_token:
        # If we have an access token, use the old approach
        logger.info("Using provided access token from environment")
        backfill = StravaBackfill(access_token=access_token, db_path=args.db)
    else:
        # If not, try OAuth approach with refresh token
        logger.info("Using OAuth approach with refresh token")
        
        # Get refresh token
        refresh_token = get_refresh_token(args.db)
        
        if not client_id or not client_secret:
            logger.error("Missing Strava credentials. Check your .env file.")
            return 1
            
        if not refresh_token:
            logger.error("No refresh token found. Please authenticate with Strava first.")
            return 1
            
        # Convert client_id to int as required by the API
        client_id = int(client_id)
            
        # Initialize the backfill with OAuth credentials
        backfill = StravaBackfill(
            db_path=args.db,
            client_id=client_id, 
            client_secret=client_secret, 
            refresh_token=refresh_token
        )
    
    try:
        if args.mode in ['both', 'efforts']:
            processed_activities = backfill.backfill_segment_efforts(args.activities)
            logger.info(f"Processed segment efforts for {processed_activities} activities")
        
        if args.mode in ['both', 'segments']:
            processed_segments = backfill.backfill_segment_details(args.segments)
            logger.info(f"Processed details for {processed_segments} segments")
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error during backfill: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    finally:
        backfill.close()
    
    return 0

if __name__ == '__main__':
    exit(main())
