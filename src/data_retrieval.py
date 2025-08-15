import time
import requests
from typing import List, Dict, Any, Optional
import logging

from src.settings import (
    STRAVA_API_BASE,
    RATE_LIMIT_REQUESTS,
    RATE_LIMIT_PERIOD,
    DEFAULT_ACTIVITY_LIMIT
)
from src.auth import get_access_token

# Set up logging
logger = logging.getLogger(__name__)

# Rate limiting variables
last_request_time = 0
request_count = 0

def rate_limit_request():
    """Implement rate limiting to stay within Strava API limits"""
    global last_request_time, request_count
    
    current_time = time.time()
    time_passed = current_time - last_request_time
    
    # Reset counter if the rate limit period has passed
    if time_passed > RATE_LIMIT_PERIOD:
        last_request_time = current_time
        request_count = 0
    
    # If approaching rate limit, sleep until reset
    if request_count >= RATE_LIMIT_REQUESTS:
        sleep_time = RATE_LIMIT_PERIOD - time_passed
        if sleep_time > 0:
            logger.info(f"Rate limit approached, sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            last_request_time = time.time()
            request_count = 0
    
    request_count += 1

def make_api_request(endpoint: str, params: Optional[Dict] = None, method: str = "GET") -> Dict:
    """Make a rate-limited request to the Strava API"""
    rate_limit_request()
    
    url = f"{STRAVA_API_BASE}{endpoint}"
    headers = {"Authorization": f"Bearer {get_access_token()}"}
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, params=params)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=params)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        if hasattr(e, 'response') and e.response is not None and hasattr(e.response, 'text'):
            logger.error(f"Response: {e.response.text}")
        raise

def get_activities(limit: int = DEFAULT_ACTIVITY_LIMIT, after_date: Optional[int] = None) -> List[Dict]:
    """
    Retrieve activities from Strava
    
    Args:
        limit: Maximum number of activities to retrieve
        after_date: Unix timestamp to filter activities after
    
    Returns:
        List of activities
    """
    params = {"per_page": 30, "page": 1}
    if after_date:
        params["after"] = after_date
    
    activities = []
    while len(activities) < limit:
        page_activities = make_api_request("/athlete/activities", params)
        
        if not page_activities:
            break
            
        activities.extend(page_activities)
        params["page"] += 1
        
        if len(page_activities) < 30:  # Less than a full page, we've reached the end
            break
    
    return activities[:limit]

def get_activity_details(activity_id: int) -> Dict:
    """
    Get detailed information about a specific activity
    
    Args:
        activity_id: Strava activity ID
        
    Returns:
        Activity details
    """
    return make_api_request(f"/activities/{activity_id}")

def get_segment_efforts(activity_id: int) -> List[Dict]:
    """
    Get segment efforts for a specific activity
    
    Args:
        activity_id: Strava activity ID
        
    Returns:
        List of segment efforts
    """
    activity = get_activity_details(activity_id)
    return activity.get("segment_efforts", [])

def get_segment_details(segment_id: int) -> Dict:
    """
    Get detailed information about a specific segment
    
    Args:
        segment_id: Strava segment ID
        
    Returns:
        Segment details
    """
    return make_api_request(f"/segments/{segment_id}")

def get_segment_streams(segment_id: int) -> Dict[str, List]:
    """
    Get streams data for a specific segment
    
    Args:
        segment_id: Strava segment ID
        
    Returns:
        Dictionary of stream data
    """
    streams = make_api_request(f"/segments/{segment_id}/streams", {
        "keys": "latlng,distance,altitude",
        "key_by_type": True
    })
    
    return streams

if __name__ == "__main__":
    # Test the data retrieval functions
    logging.basicConfig(level=logging.INFO)
    print("Fetching your recent activities...")
    
    try:
        activities = get_activities(5)
        print(f"Retrieved {len(activities)} activities")
        
        if activities:
            activity = activities[0]
            print(f"Latest activity: {activity['name']} on {activity['start_date']}")
            
            efforts = get_segment_efforts(activity['id'])
            print(f"Found {len(efforts)} segment efforts in this activity")
            
            if efforts:
                segment_id = efforts[0]['segment']['id']
                segment = get_segment_details(segment_id)
                print(f"Segment details: {segment['name']}, {segment['distance']}m, {segment['average_grade']}% grade")
    except Exception as e:
        print(f"Error: {e}")
