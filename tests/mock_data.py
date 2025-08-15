"""
Mock data for testing Strava API functionality.
"""
import json
import datetime
from typing import Dict, Any, List

# Example activity response
MOCK_ACTIVITY = {
    "id": 12345678987654321,
    "name": "Morning Run",
    "distance": 7250.2,
    "moving_time": 2100,
    "elapsed_time": 2200,
    "total_elevation_gain": 110.0,
    "type": "Run",
    "sport_type": "Run",
    "start_date": "2023-05-01T08:00:00Z",
    "start_date_local": "2023-05-01T09:00:00Z",
    "timezone": "(GMT+01:00) Europe/London",
    "start_latlng": [51.5, -0.1],
    "end_latlng": [51.5, -0.1],
    "achievement_count": 2,
    "average_speed": 3.45,
    "max_speed": 5.2,
    "average_watts": 185.0,
    "has_heartrate": True,
    "average_heartrate": 152.0,
    "max_heartrate": 175.0
}

# Example segment effort
MOCK_SEGMENT_EFFORT = {
    "id": 987654321123456,
    "activity_id": 12345678987654321,
    "elapsed_time": 155,
    "moving_time": 155,
    "start_date": "2023-05-01T08:10:00Z",
    "start_date_local": "2023-05-01T09:10:00Z",
    "distance": 1250.0,
    "average_watts": 210.0,
    "device_watts": True,
    "average_heartrate": 160.0,
    "max_heartrate": 172.0,
    "pr_rank": 1,
    "achievements": [
        {
            "type_id": 3,
            "type": "pr",
            "rank": 1
        }
    ],
    "segment": {
        "id": 12345678,
        "name": "Test Segment",
        "activity_type": "Run",
        "distance": 1250.0,
        "average_grade": 3.2,
        "maximum_grade": 5.8,
        "elevation_high": 120.0,
        "elevation_low": 80.0,
        "start_latlng": [51.5, -0.1],
        "end_latlng": [51.51, -0.09],
        "climb_category": 0,
        "city": "London",
        "state": "England",
        "country": "United Kingdom",
        "private": False,
        "starred": True
    }
}

# Example segment details
MOCK_SEGMENT = {
    "id": 12345678,
    "name": "Test Segment",
    "activity_type": "Run",
    "distance": 1250.0,
    "average_grade": 3.2,
    "maximum_grade": 5.8,
    "elevation_high": 120.0,
    "elevation_low": 80.0,
    "start_latlng": [51.5, -0.1],
    "end_latlng": [51.51, -0.09],
    "climb_category": 0,
    "city": "London",
    "state": "England",
    "country": "United Kingdom",
    "effort_count": 10265,
    "athlete_count": 5624,
    "hazardous": False,
    "star_count": 342,
    "private": False,
    "starred": True,
    "fetched_at": datetime.datetime.now().isoformat(),
    "map": {
        "id": "s12345678",
        "polyline": "ki{eFvpcbCqAoK_DsWiG}g@",
        "resource_state": 3
    },
    "coordinate_points": "ki{eFvpcbCqAoK_DsWiG}g@"
}

# List of activities for testing
MOCK_ACTIVITIES = [
    MOCK_ACTIVITY,
    {**MOCK_ACTIVITY, "id": 12345678987654322, "name": "Evening Ride", "type": "Ride", "sport_type": "Ride", "distance": 15400.5}
]

# Mock segment efforts
MOCK_SEGMENT_EFFORTS = [
    MOCK_SEGMENT_EFFORT,
    {**MOCK_SEGMENT_EFFORT, "id": 987654321123457, "elapsed_time": 162, "start_date": "2023-05-08T08:10:00Z"},
    {**MOCK_SEGMENT_EFFORT, "id": 987654321123458, "elapsed_time": 148, "start_date": "2023-05-15T08:10:00Z"}
]

# Mock segment streams
MOCK_SEGMENT_STREAMS = {
    "distance": {
        "data": [0.0, 125.0, 250.0, 375.0, 500.0, 625.0, 750.0, 875.0, 1000.0, 1125.0, 1250.0],
        "series_type": "distance",
        "original_size": 11,
        "resolution": "high"
    },
    "latlng": {
        "data": [
            [51.5, -0.1],
            [51.501, -0.099],
            [51.502, -0.098],
            [51.503, -0.097],
            [51.504, -0.096],
            [51.505, -0.095],
            [51.506, -0.094],
            [51.507, -0.093],
            [51.508, -0.092],
            [51.509, -0.091],
            [51.51, -0.09]
        ],
        "series_type": "latlng",
        "original_size": 11,
        "resolution": "high"
    },
    "altitude": {
        "data": [80.0, 84.0, 88.0, 92.0, 96.0, 100.0, 104.0, 108.0, 112.0, 116.0, 120.0],
        "series_type": "altitude",
        "original_size": 11,
        "resolution": "high"
    }
}
