#!/usr/bin/env python3
"""
Utility script to mark specific activities as processed to avoid them
being repeatedly processed even when they have no segments.
"""

import sqlite3
import argparse
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def mark_activity_processed(db_path, activity_id):
    """Mark an activity as having its segment efforts processed"""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # First check if the activity exists
        cursor.execute("SELECT id, name FROM activities WHERE id = ?", (activity_id,))
        activity = cursor.fetchone()
        
        if not activity:
            logger.error(f"Activity {activity_id} not found in database")
            return False
            
        # Check if already processed
        cursor.execute("SELECT segment_efforts_processed FROM activities WHERE id = ?", (activity_id,))
        processed = cursor.fetchone()[0]
        
        if processed == 1:
            logger.info(f"Activity {activity_id} is already marked as processed")
            return True
        
        # Mark activity as processed
        cursor.execute("""
            UPDATE activities 
            SET segment_efforts_processed = 1
            WHERE id = ?
        """, (activity_id,))
        conn.commit()
        
        # Confirm update
        cursor.execute("SELECT segment_efforts_processed FROM activities WHERE id = ?", (activity_id,))
        processed = cursor.fetchone()[0]
        
        if processed == 1:
            logger.info(f"Successfully marked activity {activity_id} as processed")
            return True
        else:
            logger.error(f"Failed to mark activity {activity_id} as processed")
            return False
            
    except Exception as e:
        logger.error(f"Error: {e}")
        return False
    finally:
        if conn:
            conn.close()

def list_activities_for_processing(db_path, limit=20):
    """List activities that need processing but have no segment efforts"""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Enable row factory to access columns by name
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT a.id, a.name, a.start_date, a.type, a.segment_efforts_processed
            FROM activities a
            LEFT JOIN (
                SELECT activity_id, COUNT(*) as effort_count
                FROM segment_efforts
                GROUP BY activity_id
            ) e ON a.id = e.activity_id
            WHERE (e.effort_count IS NULL OR e.effort_count = 0)
            AND (a.segment_efforts_processed IS NULL OR a.segment_efforts_processed = 0)
            ORDER BY a.start_date DESC
            LIMIT ?
        """, (limit,))
        
        activities = cursor.fetchall()
        
        if not activities:
            logger.info("No activities found that need processing but have no segment efforts")
            return
            
        print("\nActivities with no segment efforts that need to be processed:")
        print("-" * 80)
        print(f"{'ID':<15} {'Name':<30} {'Date':<20} {'Type':<15}")
        print("-" * 80)
        
        for activity in activities:
            print(f"{activity['id']:<15} {activity['name'][:28]:<30} {activity['start_date'][:19]:<20} {activity['type']:<15}")
            
        print("\nTo mark these as processed, run:")
        print(f"python {sys.argv[0]} --mark ID1 ID2 ID3 ...")
        
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        if conn:
            conn.close()

def mark_all_zero_segment_activities(db_path):
    """Mark all activities with zero segments as processed"""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Find all activities with no segment efforts and not yet processed
        cursor.execute("""
            UPDATE activities
            SET segment_efforts_processed = 1
            WHERE id IN (
                SELECT a.id
                FROM activities a
                LEFT JOIN (
                    SELECT activity_id, COUNT(*) as effort_count
                    FROM segment_efforts
                    GROUP BY activity_id
                ) e ON a.id = e.activity_id
                WHERE (e.effort_count IS NULL OR e.effort_count = 0)
                AND (a.segment_efforts_processed IS NULL OR a.segment_efforts_processed = 0)
            )
        """)
        
        count = cursor.rowcount
        conn.commit()
        
        logger.info(f"Marked {count} activities with no segment efforts as processed")
        return count
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return 0
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='Mark activities as processed to avoid reprocessing')
    parser.add_argument('--db', type=str, default='data/segments.db',
                        help='Path to the SQLite database')
    parser.add_argument('--list', action='store_true',
                        help='List activities that need processing but have no segment efforts')
    parser.add_argument('--mark', nargs='+', type=int,
                        help='Mark specific activities as processed (space separated IDs)')
    parser.add_argument('--mark-all-zero', action='store_true',
                        help='Mark all activities with zero segments as processed')
                        
    args = parser.parse_args()
    
    if args.list:
        list_activities_for_processing(args.db)
    elif args.mark:
        success_count = 0
        for activity_id in args.mark:
            if mark_activity_processed(args.db, activity_id):
                success_count += 1
        logger.info(f"Successfully marked {success_count} out of {len(args.mark)} activities as processed")
    elif args.mark_all_zero:
        mark_all_zero_segment_activities(args.db)
    else:
        parser.print_help()

if __name__ == '__main__':
    sys.exit(main())
