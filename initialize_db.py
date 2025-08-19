#!/usr/bin/env python3
"""
Initialize the database with the necessary tables for the segments-unlocked application.
This script creates the activities, segments, and segment_efforts tables if they don't exist.
"""

import os
import sqlite3
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def initialize_database(db_path):
    """Initialize the database with required tables"""
    logger.info(f"Initializing database: {db_path}")
    
    # Connect to the database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Create activities table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS activities (
            id INTEGER PRIMARY KEY,
            name TEXT,
            description TEXT,
            athlete_id INTEGER,
            distance REAL,
            moving_time INTEGER,
            elapsed_time INTEGER,
            total_elevation_gain REAL,
            type TEXT,
            start_date TEXT,
            start_date_local TEXT,
            timezone TEXT,
            utc_offset REAL,
            map_summary_polyline TEXT,
            average_speed REAL,
            max_speed REAL,
            average_heartrate REAL,
            max_heartrate REAL,
            has_heartrate BOOLEAN,
            calories REAL,
            device_name TEXT,
            segment_efforts_processed INTEGER DEFAULT 0
        )
        """)
        logger.info("Created activities table")
        
        # Create segments table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS segments (
            id INTEGER PRIMARY KEY,
            name TEXT,
            activity_type TEXT,
            distance REAL,
            average_grade REAL,
            maximum_grade REAL,
            elevation_high REAL,
            elevation_low REAL,
            total_elevation_gain REAL,
            effort_count INTEGER,
            athlete_count INTEGER,
            star_count INTEGER,
            city TEXT,
            state TEXT,
            country TEXT,
            private BOOLEAN,
            hazardous BOOLEAN,
            created_at TEXT,
            updated_at TEXT,
            map_polyline TEXT
        )
        """)
        logger.info("Created segments table")
        
        # Create segment_efforts table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS segment_efforts (
            id INTEGER PRIMARY KEY,
            activity_id INTEGER,
            segment_id INTEGER,
            name TEXT,
            elapsed_time INTEGER,
            moving_time INTEGER,
            start_date TEXT,
            start_date_local TEXT,
            distance REAL,
            average_watts REAL,
            device_watts BOOLEAN,
            average_heartrate REAL,
            max_heartrate REAL,
            pr_rank INTEGER,
            achievements INTEGER,
            start_index INTEGER,
            end_index INTEGER,
            kom_rank INTEGER,
            FOREIGN KEY (activity_id) REFERENCES activities (id),
            FOREIGN KEY (segment_id) REFERENCES segments (id)
        )
        """)
        logger.info("Created segment_efforts table")
        
        # Create indices for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_segment_efforts_activity_id ON segment_efforts (activity_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_segment_efforts_segment_id ON segment_efforts (segment_id)")
        logger.info("Created indices")
        
        conn.commit()
        logger.info("Database initialization completed successfully")
        return True
    
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        conn.rollback()
        return False
    
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Initialize database for segments-unlocked')
    parser.add_argument('--db', type=str, default='storage.db',
                        help='Path to the SQLite database')
    
    args = parser.parse_args()
    
    # Create directory if it doesn't exist
    db_dir = os.path.dirname(args.db)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    success = initialize_database(args.db)
    return 0 if success else 1

if __name__ == '__main__':
    exit(main())
