#!/usr/bin/env python3
import sqlite3
import os
import sys

def clean_ride_activities():
    """
    Remove all 'Ride' activities and their associated segment efforts from the database.
    This is a one-time cleanup script to remove unwanted activity types.
    """
    # Connect to the database
    db_path = os.path.join(os.path.dirname(__file__), 'data', 'segments.db')
    if not os.path.exists(db_path):
        print(f"Database file not found at {db_path}")
        return False
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    try:
        # Begin transaction
        conn.execute("BEGIN TRANSACTION")
        
        # Get the count of Ride activities
        cursor = conn.execute("SELECT COUNT(*) FROM activities WHERE type = 'Ride'")
        ride_count = cursor.fetchone()[0]
        print(f"Found {ride_count} 'Ride' activities to remove")
        
        # Get all the Ride activity IDs
        cursor = conn.execute("SELECT id FROM activities WHERE type = 'Ride'")
        ride_ids = [row[0] for row in cursor.fetchall()]
        
        if not ride_ids:
            print("No 'Ride' activities found in the database.")
            conn.commit()
            return True
        
        # Get count of segment efforts to delete
        placeholders = ','.join(['?' for _ in ride_ids])
        query = f"SELECT COUNT(*) FROM segment_efforts WHERE activity_id IN ({placeholders})"
        cursor = conn.execute(query, ride_ids)
        effort_count = cursor.fetchone()[0]
        print(f"Found {effort_count} segment efforts associated with 'Ride' activities")
        
        # Ask for confirmation
        print("\nWARNING: This will permanently delete all 'Ride' activities and associated segment efforts.")
        confirm = input("Are you sure you want to continue? (yes/no): ")
        if confirm.lower() not in ['yes', 'y']:
            print("Operation cancelled.")
            conn.rollback()
            return False
        
        # Delete segment efforts associated with Ride activities
        query = f"DELETE FROM segment_efforts WHERE activity_id IN ({placeholders})"
        conn.execute(query, ride_ids)
        print(f"Deleted {effort_count} segment efforts")
        
        # Delete Ride activities
        conn.execute("DELETE FROM activities WHERE type = 'Ride'")
        print(f"Deleted {ride_count} 'Ride' activities")
        
        # Commit transaction
        conn.commit()
        print("Database cleanup completed successfully!")
        
        # Verify deletion
        cursor = conn.execute("SELECT COUNT(*) FROM activities WHERE type = 'Ride'")
        remaining = cursor.fetchone()[0]
        print(f"Remaining 'Ride' activities: {remaining}")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return False
        
    finally:
        conn.close()

if __name__ == "__main__":
    print("=== Segments Unlocked Database Cleanup ===")
    print("This script will remove all 'Ride' activities and their associated segment efforts.")
    
    success = clean_ride_activities()
    
    if success:
        print("\nYou can now run the application with only Run activities.")
    else:
        print("\nCleanup operation failed or was cancelled.")
        sys.exit(1)
