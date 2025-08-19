#!/usr/bin/env python3
"""
Import Strava archive ZIP file and prepare for segment backfill.
This script extracts activities from a Strava data export archive
and imports them into the segments database.
"""

import os
import sys
import argparse
import zipfile
import json
import csv
import sqlite3
import logging
import tempfile
from datetime import datetime
import shutil
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def ensure_dir(directory):
    """Ensure a directory exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def connect_db(db_path):
    """Connect to the SQLite database"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def import_activities_from_csv(csv_file, db_path):
    """Import activities from the Strava export CSV file"""
    
    # Connect to database
    conn = connect_db(db_path)
    cursor = conn.cursor()
    
    # Create activities table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS activities (
        id INTEGER PRIMARY KEY,
        name TEXT,
        description TEXT,
        type TEXT,
        start_date TEXT,
        start_date_local TEXT,
        elapsed_time INTEGER,
        moving_time INTEGER,
        distance REAL,
        total_elevation_gain REAL,
        average_speed REAL,
        max_speed REAL,
        average_cadence REAL,
        average_watts REAL,
        max_watts REAL,
        weighted_average_watts REAL,
        kilojoules REAL,
        device_watts INTEGER,
        has_heartrate INTEGER,
        average_heartrate REAL,
        max_heartrate REAL,
        max_cadence INTEGER,
        pr_count INTEGER,
        total_photo_count INTEGER,
        achievement_count INTEGER,
        kudos_count INTEGER,
        comment_count INTEGER,
        athlete_count INTEGER,
        photo_count INTEGER,
        trainer INTEGER,
        commute INTEGER,
        manual INTEGER,
        private INTEGER,
        flagged INTEGER,
        workout_type INTEGER,
        gear_id TEXT,
        segment_efforts_processed INTEGER DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Read CSV file
    activities = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip rows without an activity ID
            if not row.get('Activity ID'):
                continue
                
            # Clean and convert data
            activity_id = int(row['Activity ID'])
            
            # Get existing activity
            cursor.execute("SELECT id FROM activities WHERE id = ?", (activity_id,))
            if cursor.fetchone():
                logger.info(f"Activity {activity_id} already exists in database, skipping")
                continue
            
            # Convert date strings
            start_date = row.get('Activity Date')
            if start_date:
                try:
                    # Parse and reformat to ISO format
                    dt = datetime.strptime(start_date, '%b %d, %Y, %I:%M:%S %p')
                    start_date = dt.isoformat()
                except ValueError:
                    # Keep as is if we can't parse it
                    pass
            
            # Create activity record
            activity = {
                'id': activity_id,
                'name': row.get('Activity Name'),
                'type': row.get('Activity Type'),
                'start_date': start_date,
                'start_date_local': start_date,  # Assume same as start_date for now
                'elapsed_time': int(float(row.get('Elapsed Time', 0))),
                'moving_time': int(float(row.get('Moving Time', 0))),
                'distance': float(row.get('Distance', 0)),
                'total_elevation_gain': float(row.get('Elevation Gain', 0)),
                'average_speed': float(row.get('Average Speed', 0)),
                'max_speed': float(row.get('Max Speed', 0)),
                'average_heartrate': float(row.get('Average Heart Rate', 0)) if row.get('Average Heart Rate') else None,
                'max_heartrate': float(row.get('Max Heart Rate', 0)) if row.get('Max Heart Rate') else None,
                'pr_count': int(row.get('PR Count', 0)) if row.get('PR Count') else 0,
                'achievement_count': int(row.get('Achievement Count', 0)) if row.get('Achievement Count') else 0,
                'kudos_count': int(row.get('Kudos', 0)) if row.get('Kudos') else 0,
                'commute': 1 if row.get('Commute') == 'true' else 0,
                'private': 1 if row.get('Visibility') == 'private' else 0
            }
            
            activities.append(activity)
            
    # Insert activities into database
    if activities:
        logger.info(f"Importing {len(activities)} activities to database")
        
        # Prepare SQL placeholders for all fields
        fields = activities[0].keys()
        placeholders = ', '.join(['?'] * len(fields))
        field_str = ', '.join(fields)
        
        # Insert activities
        for activity in activities:
            values = [activity.get(field) for field in fields]
            cursor.execute(f"INSERT INTO activities ({field_str}) VALUES ({placeholders})", values)
        
        conn.commit()
        logger.info(f"Successfully imported {len(activities)} activities")
    else:
        logger.info("No new activities found to import")
    
    conn.close()

def extract_and_import_archive(zip_file, db_path):
    """Extract relevant files from Strava ZIP archive and import them"""
    
    # Create temporary directory to extract files
    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info(f"Extracting Strava archive to temporary directory")
        
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                # Extract the activities.csv file
                for file_info in zip_ref.infolist():
                    if 'activities.csv' in file_info.filename.lower():
                        logger.info(f"Found activities file: {file_info.filename}")
                        zip_ref.extract(file_info, temp_dir)
                        csv_path = os.path.join(temp_dir, file_info.filename)
                        import_activities_from_csv(csv_path, db_path)
                        break
                else:
                    logger.error("No activities.csv file found in the archive")
                    return False
                
        except zipfile.BadZipFile:
            logger.error(f"Invalid ZIP file: {zip_file}")
            return False
        except Exception as e:
            logger.error(f"Error extracting archive: {e}")
            return False
            
    return True

def main():
    parser = argparse.ArgumentParser(description='Import Strava archive and prepare for segment backfill')
    parser.add_argument('zip_file', help='Path to the Strava export ZIP file')
    parser.add_argument('--db', default='data/segments.db', help='Path to the database file')
    
    args = parser.parse_args()
    
    # Ensure the database directory exists
    db_dir = os.path.dirname(args.db)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Process the zip file
    if extract_and_import_archive(args.zip_file, args.db):
        logger.info(f"Import completed successfully. Database: {args.db}")
        logger.info("You can now run the backfill process to fetch segment efforts and details.")
        logger.info("Run: python manage_backfill.py --mode stats")
        return 0
    else:
        logger.error("Import failed.")
        return 1

if __name__ == '__main__':
    exit(main())
