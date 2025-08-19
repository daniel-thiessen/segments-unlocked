#!/usr/bin/env python3
"""
Updates the database schema to add necessary fields for incremental backfill.
Creates tables if they don't exist or updates existing ones.
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

def update_schema(db_path):
    """Update the database schema for incremental backfill support"""
    logger.info(f"Updating schema in database: {db_path}")
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check if activities table exists, create it if it doesn't
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='activities'")
        if not cursor.fetchone():
            logger.info("Activities table not found, creating it")
            cursor.execute("""
            CREATE TABLE activities (
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
        else:
            # Check if the segment_efforts_processed column exists
            cursor.execute("PRAGMA table_info(activities)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'segment_efforts_processed' not in columns:
                logger.info("Adding segment_efforts_processed column to activities table")
                cursor.execute("ALTER TABLE activities ADD COLUMN segment_efforts_processed INTEGER DEFAULT 0")
        
        # Check if segments table exists, create it if it doesn't
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='segments'")
        if not cursor.fetchone():
            logger.info("Segments table not found, creating it")
            cursor.execute("""
            CREATE TABLE segments (
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

        # Check if segment_efforts table exists, create it if it doesn't
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='segment_efforts'")
        if not cursor.fetchone():
            logger.info("Segment_efforts table not found, creating it")
            cursor.execute("""
            CREATE TABLE segment_efforts (
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
            
        # Create indices for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_segment_efforts_segment_id ON segment_efforts (segment_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_segment_efforts_activity_id ON segment_efforts (activity_id)")
        
        conn.commit()
        logger.info("Schema update completed successfully")
        return True
    
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        conn.rollback()
        return False
    
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Update database schema for incremental backfill')
    parser.add_argument('--db', type=str, default='data/segments.db',
                        help='Path to the SQLite database')
    parser.add_argument('--create', action='store_true',
                        help='Create the database file if it does not exist')
    
    args = parser.parse_args()
    
    # Check if database file exists
    if not os.path.exists(args.db):
        if args.create:
            logger.info(f"Database file not found, will be created: {args.db}")
            # Ensure directory exists
            db_dir = os.path.dirname(args.db)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
        else:
            logger.error(f"Database file not found: {args.db}")
            logger.error(f"Use --create flag to create a new database")
            return 1
    
    success = update_schema(args.db)
    return 0 if success else 1

if __name__ == '__main__':
    exit(main())
