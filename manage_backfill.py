#!/usr/bin/env python3
"""
Manages the backfill process for Strava data, providing different strategies:
1. One-time full backfill
2. Continuous incremental backfill with configurable intervals
"""

import os
import time
import argparse
import logging
import subprocess
import signal
import sys
import sqlite3
import json
from datetime import datetime

from incremental_backfill import StravaBackfill, load_env, get_refresh_token

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("backfill.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Global flag to handle graceful shutdown
running = True

def signal_handler(sig, frame):
    """Handle interrupt signals"""
    global running
    logger.info("Received shutdown signal, finishing current cycle...")
    running = False

def run_command(command):
    """Run a shell command and return exit code"""
    logger.info(f"Running command: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"Command failed with exit code {result.returncode}")
        logger.error(f"Error: {result.stderr}")
    else:
        logger.info(f"Command succeeded: {result.stdout.strip()}")
    
    return result.returncode

def ensure_schema_updated(db_path):
    """Ensure the database schema is updated for backfill"""
    logger.info("Ensuring database schema is updated")
    exit_code = run_command(f"python update_schema.py --db {db_path}")
    if exit_code != 0:
        logger.error("Failed to update database schema")
        return False
    return True

def save_state(state_file, state):
    """Save the backfill state to a file"""
    with open(state_file, 'w') as f:
        json.dump(state, f)
    logger.debug(f"Saved state to {state_file}")

def load_state(state_file):
    """Load the backfill state from a file"""
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
            logger.debug(f"Loaded state from {state_file}")
            return state
        except Exception as e:
            logger.error(f"Error loading state: {e}")
    
    # Default state
    return {
        'last_run': None,
        'activities_processed': 0,
        'segments_processed': 0,
        'total_efforts': 0
    }

def get_db_stats(db_path):
    """Get statistics about the database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    stats = {}
    
    try:
        # Count activities
        cursor.execute("SELECT COUNT(*) FROM activities")
        stats['total_activities'] = cursor.fetchone()[0]
        
        # Count processed activities
        cursor.execute("SELECT COUNT(*) FROM activities WHERE segment_efforts_processed = 1")
        stats['processed_activities'] = cursor.fetchone()[0]
        
        # Count segment efforts
        cursor.execute("SELECT COUNT(*) FROM segment_efforts")
        stats['segment_efforts'] = cursor.fetchone()[0]
        
        # Count segments with details
        cursor.execute("SELECT COUNT(*) FROM segments")
        stats['segments'] = cursor.fetchone()[0]
        
        # Count segments that need details
        cursor.execute("""
            SELECT COUNT(DISTINCT e.segment_id)
            FROM segment_efforts e
            LEFT JOIN segments s ON e.segment_id = s.id
            WHERE s.id IS NULL
        """)
        stats['segments_needing_details'] = cursor.fetchone()[0]
        
        # Count activities needing segment efforts
        cursor.execute("""
            SELECT COUNT(*)
            FROM activities a
            WHERE a.segment_efforts_processed IS NULL OR a.segment_efforts_processed = 0
        """)
        stats['activities_needing_processing'] = cursor.fetchone()[0]
        
    except Exception as e:
        logger.error(f"Error getting DB stats: {e}")
    
    conn.close()
    return stats

def print_stats(db_path, state):
    """Print statistics about the backfill process and database"""
    stats = get_db_stats(db_path)
    
    print("\n===== Backfill Statistics =====")
    print(f"Last run: {state.get('last_run') or 'Never'}")
    print(f"Activities processed: {state.get('activities_processed', 0)}")
    print(f"Segments processed: {state.get('segments_processed', 0)}")
    
    print("\n===== Database Statistics =====")
    print(f"Total activities: {stats.get('total_activities', 0)}")
    print(f"Processed activities: {stats.get('processed_activities', 0)} ({stats.get('activities_needing_processing', 0)} remaining)")
    print(f"Total segment efforts: {stats.get('segment_efforts', 0)}")
    print(f"Segments with details: {stats.get('segments', 0)}")
    print(f"Segments needing details: {stats.get('segments_needing_details', 0)}")
    
    # Calculate completion percentages
    if stats.get('total_activities', 0) > 0:
        progress = (stats.get('processed_activities', 0) / stats.get('total_activities', 1)) * 100
        print(f"\nActivity processing: {progress:.1f}% complete")
    
    if stats.get('segments', 0) > 0 and stats.get('segments_needing_details', 0) > 0:
        segment_progress = (stats.get('segments', 0) / (stats.get('segments', 0) + stats.get('segments_needing_details', 0))) * 100
        print(f"Segment details: {segment_progress:.1f}% complete")

def one_time_backfill(backfill, activities_per_batch, segments_per_batch, state_file):
    """Perform a one-time full backfill using direct backfill object"""
    logger.info("Starting one-time full backfill")
    
    state = load_state(state_file)
    
    try:
        # First, process all activities to get segment efforts
        logger.info("Processing segment efforts for all activities")
        processed_activities = backfill.backfill_segment_efforts(activities_per_batch)
        state['activities_processed'] += processed_activities
        
        # Then, process all segments to get segment details
        logger.info("Processing segment details for all segments")
        processed_segments = backfill.backfill_segment_details(segments_per_batch)
        state['segments_processed'] += processed_segments
        
        # Update state
        state['last_run'] = datetime.now().isoformat()
        save_state(state_file, state)
        
        logger.info(f"One-time backfill completed: processed {processed_activities} activities and {processed_segments} segments")
    except Exception as e:
        logger.error(f"Error in one-time backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        print_stats(backfill.db.db_path, state)
        
def continuous_backfill(backfill, activities_per_batch, segments_per_batch, 
                        check_interval, max_runs, state_file):
    """Perform a continuous incremental backfill with configurable intervals using direct backfill object"""
    global running
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    state = load_state(state_file)
    
    logger.info(f"Starting continuous backfill process (interval: {check_interval}s)")
    logger.info(f"Processing up to {activities_per_batch} activities and {segments_per_batch} segments per cycle")
    
    runs = 0
    
    try:
        while running:
            start_time = time.time()
            
            # Process activities
            processed_activities = backfill.backfill_segment_efforts(activities_per_batch)
            state['activities_processed'] += processed_activities
            
            # Process segments
            processed_segments = backfill.backfill_segment_details(segments_per_batch)
            state['segments_processed'] += processed_segments
            
            # Update state
            state['last_run'] = datetime.now().isoformat()
            save_state(state_file, state)
            
            # Log progress
            if processed_activities > 0 or processed_segments > 0:
                logger.info(f"Cycle complete: processed {processed_activities} activities and {processed_segments} segments")
            else:
                logger.info("Cycle complete: nothing to process")
                
            # Increment run counter
            runs += 1
            
            # Check if we've reached the maximum number of runs
            if max_runs > 0 and runs >= max_runs:
                logger.info(f"Reached maximum number of runs: {max_runs}")
                break
                
            # Check if we should exit based on completion
            stats = get_db_stats(backfill.db.db_path)
            if stats.get('activities_needing_processing', 0) == 0 and stats.get('segments_needing_details', 0) == 0:
                logger.info("All activities and segments processed! Exiting.")
                break
            
            # Calculate wait time (respect the interval)
            elapsed = time.time() - start_time
            wait_time = max(0, check_interval - elapsed)
            
            if wait_time > 0 and running:
                logger.info(f"Waiting {wait_time:.1f}s until next cycle...")
                time.sleep(wait_time)
    
    except Exception as e:
        logger.error(f"Error in continuous backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    finally:
        # Final state update
        state['last_run'] = datetime.now().isoformat()
        save_state(state_file, state)
        print_stats(backfill.db.db_path, state)

def main():
    parser = argparse.ArgumentParser(description='Manage Strava data backfill process')
    parser.add_argument('--mode', choices=['one-time', 'continuous', 'stats'], default='one-time',
                        help='Backfill mode: one-time, continuous, or just show stats')
    parser.add_argument('--db', type=str, default='data/segments.db',
                        help='Path to the SQLite database')
    parser.add_argument('--env', type=str, default='.env',
                        help='Path to the .env file with Strava credentials')
    parser.add_argument('--state', type=str, default='backfill_state.json',
                        help='Path to the state file to track progress')
    parser.add_argument('--activities', type=int, default=10,
                        help='Number of activities to process per batch')
    parser.add_argument('--segments', type=int, default=20,
                        help='Number of segments to process per batch')
    parser.add_argument('--interval', type=int, default=300,
                        help='Interval between backfill cycles in seconds (continuous mode only)')
    parser.add_argument('--max-runs', type=int, default=0,
                        help='Maximum number of runs (0 for unlimited, continuous mode only)')
    
    args = parser.parse_args()
    
    # Show stats only?
    if args.mode == 'stats':
        state = load_state(args.state)
        print_stats(args.db, state)
        return 0
    
    # Ensure database schema is updated
    if not ensure_schema_updated(args.db):
        return 1
    
    # Load environment variables from .env file
    env_vars = load_env(args.env)
    
    # Get Strava credentials
    client_id = env_vars.get('STRAVA_CLIENT_ID')
    client_secret = env_vars.get('STRAVA_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        logger.error("Missing Strava credentials. Check your .env file.")
        return 1
    
    # Try to get access token directly first
    access_token = os.environ.get('STRAVA_ACCESS_TOKEN')
    
    if access_token:
        # If we have an access token, use it directly
        logger.info("Using provided access token from environment")
        try:
            backfill = StravaBackfill(access_token=access_token, db_path=args.db)
        except Exception as e:
            logger.error(f"Failed to initialize backfill with access token: {e}")
            return 1
    else:
        # If no access token, try OAuth approach with refresh token
        logger.info("Using OAuth approach with refresh token")
        
        # Try to get refresh token from .env, then from database
        refresh_token = env_vars.get('STRAVA_REFRESH_TOKEN')
        if not refresh_token:
            refresh_token = get_refresh_token(args.db)
        
        if not refresh_token:
            logger.error("No refresh token found. Please authenticate with Strava first.")
            return 1
            
        # Convert client_id to int as required by the API
        try:
            client_id = int(client_id)
        except ValueError:
            logger.error(f"Invalid client ID: {client_id}. Must be an integer.")
            return 1
            
        # Initialize the backfill with OAuth credentials
        try:
            backfill = StravaBackfill(
                db_path=args.db,
                client_id=client_id,
                client_secret=client_secret,
                refresh_token=refresh_token
            )
        except Exception as e:
            logger.error(f"Failed to initialize backfill with OAuth: {e}")
            return 1
    
    try:
        # Run in selected mode
        if args.mode == 'one-time':
            one_time_backfill(backfill, args.activities, args.segments, args.state)
        else:  # continuous
            continuous_backfill(backfill, args.activities, args.segments, args.interval, args.max_runs, args.state)
    finally:
        backfill.close()
    
    return 0

if __name__ == '__main__':
    exit(main())
