"""
Test script for timestamp_utils.py
"""
import logging
import sys
import os

# Add parent directory to path to enable imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.timestamp_utils import get_latest_activity_timestamp
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)

def test_get_latest_activity_timestamp():
    """Test retrieving the latest activity timestamp"""
    timestamp = get_latest_activity_timestamp()
    
    if timestamp:
        date_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        print(f"Latest activity timestamp: {timestamp}")
        print(f"This corresponds to: {date_str}")
        
        # Show how to use this in API calls
        print(f"\nUsage example for Strava API:")
        print(f"  Parameter: after={timestamp}")
        print(f"  Will fetch activities after {date_str}")
    else:
        print("No activities found in database.")

if __name__ == "__main__":
    test_get_latest_activity_timestamp()
