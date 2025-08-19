#!/usr/bin/env python3
"""
This script tests retrieving a single segment and displaying its coordinate data.
"""

import os
import sys
import json
from src.storage import SegmentDatabase
from src.data_retrieval import get_segment_details
from src.auth import authenticate

def test_segment_location_retrieval():
    # Authenticate with Strava
    tokens = authenticate()
    if not tokens:
        print("Authentication failed")
        return
    
    # Connect to the database
    db = SegmentDatabase()
    
    try:
        # Find a segment to test
        popular_segments = db.get_popular_segments(1)
        if not popular_segments:
            print("No segments found in the database")
            return
        
        segment_id, name, count = popular_segments[0]
        print(f"Testing with segment: {name} (ID: {segment_id}, {count} efforts)")
        
        # Check if it already has coordinate data
        existing_segment = db.get_segment_by_id(segment_id)
        has_coordinates = False
        coordinate_points = None
        
        if existing_segment is not None:
            coordinate_points = existing_segment.get('coordinate_points')
            has_coordinates = coordinate_points is not None and coordinate_points != ''
        
        print(f"Already has coordinate data: {has_coordinates}")
        
        if has_coordinates and coordinate_points:
            print("Coordinates (first 100 chars):", coordinate_points[:100], "...")
        
        # Fetch from API
        print(f"Fetching segment {segment_id} from Strava API...")
        segment_detail = get_segment_details(segment_id)
        
        # Check for coordinate data
        coordinate_data = segment_detail.get('map', {}).get('polyline')
        if coordinate_data:
            print(f"Successfully retrieved coordinate data (first 100 chars): {coordinate_data[:100]} ...")
            # Update the segment in the database
            db.save_segment(segment_detail)
            print(f"Updated segment {segment_id} in the database with new coordinate data")
        else:
            print(f"No coordinate data returned for segment {segment_id}")
    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    test_segment_location_retrieval()
