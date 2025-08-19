from src.storage import SegmentDatabase
from datetime import datetime
import time

def get_latest_activity_timestamp():
    """Get the timestamp of the latest activity in the database"""
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
            print(f"Latest activity: {start_date} ({activity['name']})")
            print(f"Unix timestamp: {timestamp}")
            return timestamp
        else:
            print("No activities found in database")
            return None
    finally:
        db.close()

if __name__ == "__main__":
    get_latest_activity_timestamp()
