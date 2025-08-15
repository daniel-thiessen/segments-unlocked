import time
import requests
import random
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
    if request_count >= RATE_LIMIT_REQUESTS - 10:  # Leave some buffer
        sleep_time = RATE_LIMIT_PERIOD - time_passed + 5  # Add 5 seconds buffer
        if sleep_time > 0:
            logger.info(f"Rate limit approached, sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            last_request_time = time.time()
            request_count = 0
    
    # Add small random delay between requests to avoid bursts
    time.sleep(random.uniform(0.1, 0.3))
    request_count += 1

def make_api_request(endpoint: str, params: Optional[Dict] = None, method: str = "GET", max_retries: int = 3) -> Any:
    """Make a rate-limited request to the Strava API with retry logic"""
    url = f"{STRAVA_API_BASE}{endpoint}"
    headers = {"Authorization": f"Bearer {get_access_token()}"}
    
    last_exception = None
    for retry in range(max_retries):
        try:
            # Apply rate limiting before each request
            rate_limit_request()
            
            if method == "GET":
                response = requests.get(url, headers=headers, params=params)
            elif method == "POST":
                response = requests.post(url, headers=headers, json=params)
            else:
                # Handle unsupported HTTP method
                error_msg = f"Unsupported HTTP method: {method}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Handle rate limit specifically
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited by Strava API, waiting {retry_after}s before retry {retry + 1}/{max_retries}")
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            last_exception = e
            logger.error(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 429:
                    retry_after = int(e.response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited by Strava API, waiting {retry_after}s before retry {retry + 1}/{max_retries}")
                    time.sleep(retry_after)
                    continue
                    
                if hasattr(e.response, 'text'):
                    logger.error(f"Response: {e.response.text}")
                    
            # If this isn't our last retry, wait and try again
            if retry < max_retries - 1:
                wait_time = (retry + 1) * 5  # Exponential backoff
                logger.info(f"Retrying in {wait_time}s (attempt {retry + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                # Out of retries, raise the exception
                if last_exception:
                    raise last_exception
                raise RuntimeError("API request failed with unknown error")
    
    # This code should never be reached, but adding it to satisfy type checker
    # and handle any unexpected logic paths
    if last_exception:
        raise last_exception
    raise RuntimeError("API request failed with unknown error - no retries attempted")

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
    
    activities: List[Dict] = []
    try:
        while len(activities) < limit:
            page_activities = make_api_request("/athlete/activities", params)
            
            if not page_activities or len(page_activities) == 0:
                logger.debug("No more activities found")
                break
                
            activities.extend(page_activities)
            params["page"] += 1
            
            if len(page_activities) < 30:  # Less than a full page, we've reached the end
                break
            
            # Add a small delay between page requests
            time.sleep(0.5)
    except Exception as e:
        logger.error(f"Error retrieving activities: {e}")
        # Return any activities we've collected so far
    
    return activities[:limit]

def get_activity_details(activity_id: int) -> Dict[str, Any]:
    """
    Get detailed information about a specific activity
    
    Args:
        activity_id: Strava activity ID
        
    Returns:
        Activity details
    """
    try:
        result = make_api_request(f"/activities/{activity_id}")
        if not isinstance(result, dict):
            logger.warning(f"Unexpected response type for activity {activity_id}: {type(result)}")
            return {}
        return result
    except Exception as e:
        logger.error(f"Error retrieving activity details for activity {activity_id}: {e}")
        raise

def get_segment_efforts(activity_id: int) -> List[Dict]:
    """
    Get segment efforts for a specific activity
    
    Args:
        activity_id: Strava activity ID
        
    Returns:
        List of segment efforts
    """
    try:
        activity = get_activity_details(activity_id)
        if not activity:
            logger.warning(f"No activity details found for activity {activity_id}")
            return []
        return activity.get("segment_efforts", [])
    except Exception as e:
        logger.error(f"Error retrieving segment efforts for activity {activity_id}: {e}")
        return []

def get_segment_details(segment_id: int) -> Dict[str, Any]:
    """
    Get detailed information about a specific segment
    
    Args:
        segment_id: Strava segment ID
        
    Returns:
        Segment details
    """
    try:
        result = make_api_request(f"/segments/{segment_id}")
        if not isinstance(result, dict):
            logger.warning(f"Unexpected response type for segment {segment_id}: {type(result)}")
            return {}
        return result
    except Exception as e:
        logger.error(f"Error retrieving segment details for segment {segment_id}: {e}")
        raise

def get_segment_streams(segment_id: int) -> Dict[str, List[Any]]:
    """
    Get streams data for a specific segment
    
    Args:
        segment_id: Strava segment ID
        
    Returns:
        Dictionary of stream data
    """
    try:
        streams = make_api_request(f"/segments/{segment_id}/streams", {
            "keys": "latlng,distance,altitude",
            "key_by_type": True
        })
        
        if not isinstance(streams, dict):
            logger.warning(f"Unexpected response type for segment streams {segment_id}: {type(streams)}")
            return {}
        return streams
    except Exception as e:
        logger.error(f"Error retrieving segment streams for segment {segment_id}: {e}")
        return {}

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
