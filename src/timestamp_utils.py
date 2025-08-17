"""
Utilities for handling timestamp-related operations.
"""

from datetime import datetime
from typing import Optional
from src.storage import SegmentDatabase
import logging

logger = logging.getLogger(__name__)

def get_latest_activity_timestamp() -> Optional[int]:
    """
    Get the timestamp of the latest activity in the database.
    
    Returns:
        Unix timestamp of the latest activity or None if no activities exist
    """
    db = SegmentDatabase()
    try:
        # Get the most recent activity
        activities = db.get_latest_activities(1)
        if activities:
            activity = activities[0]
            # Convert ISO format date to timestamp
            start_date = activity['start_date']
            dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            timestamp = int(dt.timestamp())
            logger.debug(f"Latest activity: {start_date} ({activity['name']})")
            logger.debug(f"Unix timestamp: {timestamp}")
            return timestamp
        else:
            logger.info("No activities found in database")
            return None
    except Exception as e:
        logger.error(f"Error getting latest activity timestamp: {e}")
        return None
    finally:
        db.close()
