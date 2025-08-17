import os
import sqlite3
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

from src.settings import DB_PATH

class SegmentDatabase:
    """Database manager for storing Strava segment data"""
    
    def __init__(self, db_path=DB_PATH):
        """Initialize the database connection"""
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
    
    def create_tables(self):
        """Create the necessary tables if they don't exist"""
        with self.conn:
            # Activities table
            self.conn.execute('''
            CREATE TABLE IF NOT EXISTS activities (
                id INTEGER PRIMARY KEY,
                name TEXT,
                type TEXT,
                start_date TEXT,
                distance REAL,
                moving_time INTEGER,
                elapsed_time INTEGER,
                total_elevation_gain REAL,
                average_speed REAL,
                max_speed REAL,
                average_watts REAL,
                kilojoules REAL,
                device_watts INTEGER,
                has_heartrate INTEGER,
                average_heartrate REAL,
                max_heartrate REAL,
                raw_data TEXT,
                fetched_at TEXT
            )
            ''')
            
            # Segment Efforts table
            self.conn.execute('''
            CREATE TABLE IF NOT EXISTS segment_efforts (
                id INTEGER PRIMARY KEY,
                activity_id INTEGER,
                segment_id INTEGER,
                name TEXT,
                elapsed_time INTEGER,
                moving_time INTEGER,
                start_date TEXT,
                distance REAL,
                average_watts REAL,
                device_watts INTEGER,
                average_heartrate REAL,
                max_heartrate REAL,
                pr_rank INTEGER,
                raw_data TEXT,
                FOREIGN KEY (activity_id) REFERENCES activities (id),
                FOREIGN KEY (segment_id) REFERENCES segments (id)
            )
            ''')
            
            # Segments table
            self.conn.execute('''
            CREATE TABLE IF NOT EXISTS segments (
                id INTEGER PRIMARY KEY,
                name TEXT,
                activity_type TEXT,
                distance REAL,
                average_grade REAL,
                maximum_grade REAL,
                elevation_high REAL,
                elevation_low REAL,
                start_latlng TEXT,
                end_latlng TEXT,
                climb_category INTEGER,
                city TEXT,
                state TEXT,
                country TEXT,
                private INTEGER,
                starred INTEGER,
                coordinate_points TEXT,
                raw_data TEXT,
                fetched_at TEXT
            )
            ''')
            
            # Create indices for faster querying
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_segment_efforts_segment_id ON segment_efforts (segment_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_segment_efforts_activity_id ON segment_efforts (activity_id)')
    
    def save_activity(self, activity: Dict) -> int:
        """
        Save or update an activity in the database
        
        Args:
            activity: Strava activity data
            
        Returns:
            Activity ID
        """
        now = datetime.now().isoformat()
        
        with self.conn:
            cursor = self.conn.cursor()
            
            # Check if activity already exists
            cursor.execute('SELECT id FROM activities WHERE id = ?', (activity['id'],))
            result = cursor.fetchone()
            
            if result:
                # Update existing activity
                cursor.execute('''
                UPDATE activities SET
                    name = ?,
                    type = ?,
                    start_date = ?,
                    distance = ?,
                    moving_time = ?,
                    elapsed_time = ?,
                    total_elevation_gain = ?,
                    average_speed = ?,
                    max_speed = ?,
                    average_watts = ?,
                    kilojoules = ?,
                    device_watts = ?,
                    has_heartrate = ?,
                    average_heartrate = ?,
                    max_heartrate = ?,
                    raw_data = ?,
                    fetched_at = ?
                WHERE id = ?
                ''', (
                    activity.get('name'),
                    activity.get('type'),
                    activity.get('start_date'),
                    activity.get('distance'),
                    activity.get('moving_time'),
                    activity.get('elapsed_time'),
                    activity.get('total_elevation_gain'),
                    activity.get('average_speed'),
                    activity.get('max_speed'),
                    activity.get('average_watts'),
                    activity.get('kilojoules'),
                    activity.get('device_watts', 0),
                    activity.get('has_heartrate', 0),
                    activity.get('average_heartrate'),
                    activity.get('max_heartrate'),
                    json.dumps(activity),
                    now,
                    activity['id']
                ))
            else:
                # Insert new activity
                cursor.execute('''
                INSERT INTO activities (
                    id, name, type, start_date, distance, moving_time, elapsed_time,
                    total_elevation_gain, average_speed, max_speed, average_watts,
                    kilojoules, device_watts, has_heartrate, average_heartrate,
                    max_heartrate, raw_data, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    activity['id'],
                    activity.get('name'),
                    activity.get('type'),
                    activity.get('start_date'),
                    activity.get('distance'),
                    activity.get('moving_time'),
                    activity.get('elapsed_time'),
                    activity.get('total_elevation_gain'),
                    activity.get('average_speed'),
                    activity.get('max_speed'),
                    activity.get('average_watts'),
                    activity.get('kilojoules'),
                    activity.get('device_watts', 0),
                    activity.get('has_heartrate', 0),
                    activity.get('average_heartrate'),
                    activity.get('max_heartrate'),
                    json.dumps(activity),
                    now
                ))
            
            return activity['id']
    
    def save_segment(self, segment: Dict) -> int:
        """
        Save or update a segment in the database
        
        Args:
            segment: Strava segment data
            
        Returns:
            Segment ID
        """
        now = datetime.now().isoformat()
        
        with self.conn:
            cursor = self.conn.cursor()
            
            # Check if segment already exists
            cursor.execute('SELECT id FROM segments WHERE id = ?', (segment['id'],))
            result = cursor.fetchone()
            
            # Extract and format lat/lng data
            start_latlng = json.dumps(segment.get('start_latlng')) if segment.get('start_latlng') else None
            end_latlng = json.dumps(segment.get('end_latlng')) if segment.get('end_latlng') else None
            
            # Format map points if available
            map_points = segment.get('map', {}).get('polyline')
            
            if result:
                # Update existing segment
                cursor.execute('''
                UPDATE segments SET
                    name = ?,
                    activity_type = ?,
                    distance = ?,
                    average_grade = ?,
                    maximum_grade = ?,
                    elevation_high = ?,
                    elevation_low = ?,
                    start_latlng = ?,
                    end_latlng = ?,
                    climb_category = ?,
                    city = ?,
                    state = ?,
                    country = ?,
                    private = ?,
                    starred = ?,
                    coordinate_points = ?,
                    raw_data = ?,
                    fetched_at = ?
                WHERE id = ?
                ''', (
                    segment.get('name'),
                    segment.get('activity_type'),
                    segment.get('distance'),
                    segment.get('average_grade'),
                    segment.get('maximum_grade'),
                    segment.get('elevation_high'),
                    segment.get('elevation_low'),
                    start_latlng,
                    end_latlng,
                    segment.get('climb_category'),
                    segment.get('city'),
                    segment.get('state'),
                    segment.get('country'),
                    segment.get('private', 0),
                    segment.get('starred', 0),
                    map_points,
                    json.dumps(segment),
                    now,
                    segment['id']
                ))
            else:
                # Insert new segment
                cursor.execute('''
                INSERT INTO segments (
                    id, name, activity_type, distance, average_grade, maximum_grade,
                    elevation_high, elevation_low, start_latlng, end_latlng, climb_category,
                    city, state, country, private, starred, coordinate_points, raw_data, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    segment['id'],
                    segment.get('name'),
                    segment.get('activity_type'),
                    segment.get('distance'),
                    segment.get('average_grade'),
                    segment.get('maximum_grade'),
                    segment.get('elevation_high'),
                    segment.get('elevation_low'),
                    start_latlng,
                    end_latlng,
                    segment.get('climb_category'),
                    segment.get('city'),
                    segment.get('state'),
                    segment.get('country'),
                    segment.get('private', 0),
                    segment.get('starred', 0),
                    map_points,
                    json.dumps(segment),
                    now
                ))
            
            return segment['id']
    
    def save_segment_effort(self, effort: Dict) -> int:
        """
        Save or update a segment effort in the database
        
        Args:
            effort: Strava segment effort data
            
        Returns:
            Segment effort ID
        """
        with self.conn:
            cursor = self.conn.cursor()
            
            # Check if effort already exists
            cursor.execute('SELECT id FROM segment_efforts WHERE id = ?', (effort['id'],))
            result = cursor.fetchone()
            
            # Save the segment if we have the data
            if 'segment' in effort:
                self.save_segment(effort['segment'])
            
            if result:
                # Update existing effort
                cursor.execute('''
                UPDATE segment_efforts SET
                    activity_id = ?,
                    segment_id = ?,
                    name = ?,
                    elapsed_time = ?,
                    moving_time = ?,
                    start_date = ?,
                    distance = ?,
                    average_watts = ?,
                    device_watts = ?,
                    average_heartrate = ?,
                    max_heartrate = ?,
                    pr_rank = ?,
                    raw_data = ?
                WHERE id = ?
                ''', (
                    effort.get('activity_id', effort.get('activity', {}).get('id')),
                    effort.get('segment_id', effort.get('segment', {}).get('id')),
                    effort.get('name'),
                    effort.get('elapsed_time'),
                    effort.get('moving_time'),
                    effort.get('start_date'),
                    effort.get('distance'),
                    effort.get('average_watts'),
                    effort.get('device_watts', 0),
                    effort.get('average_heartrate'),
                    effort.get('max_heartrate'),
                    effort.get('pr_rank'),
                    json.dumps(effort),
                    effort['id']
                ))
            else:
                # Insert new effort
                cursor.execute('''
                INSERT INTO segment_efforts (
                    id, activity_id, segment_id, name, elapsed_time, moving_time,
                    start_date, distance, average_watts, device_watts, 
                    average_heartrate, max_heartrate, pr_rank, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    effort['id'],
                    effort.get('activity_id', effort.get('activity', {}).get('id')),
                    effort.get('segment_id', effort.get('segment', {}).get('id')),
                    effort.get('name'),
                    effort.get('elapsed_time'),
                    effort.get('moving_time'),
                    effort.get('start_date'),
                    effort.get('distance'),
                    effort.get('average_watts'),
                    effort.get('device_watts', 0),
                    effort.get('average_heartrate'),
                    effort.get('max_heartrate'),
                    effort.get('pr_rank'),
                    json.dumps(effort)
                ))
            
            return effort['id']
    
    def get_latest_activities(self, limit=10) -> List[Dict]:
        """
        Get the most recent activities
        
        Args:
            limit: Maximum number of activities to retrieve
            
        Returns:
            List of activities
        """
        cursor = self.conn.execute(
            'SELECT * FROM activities ORDER BY start_date DESC LIMIT ?',
            (limit,)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def get_segment_efforts_by_segment(self, segment_id: int) -> List[Dict]:
        """
        Get all efforts for a specific segment
        
        Args:
            segment_id: Strava segment ID
            
        Returns:
            List of segment efforts
        """
        cursor = self.conn.execute(
            '''
            SELECT se.*, a.name as activity_name, a.type as activity_type 
            FROM segment_efforts se
            JOIN activities a ON se.activity_id = a.id
            WHERE se.segment_id = ?
            ORDER BY se.start_date DESC
            ''',
            (segment_id,)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def get_segment_by_id(self, segment_id: int) -> Optional[Dict]:
        """
        Get segment details by ID
        
        Args:
            segment_id: Strava segment ID
            
        Returns:
            Segment data or None if not found
        """
        cursor = self.conn.execute(
            'SELECT * FROM segments WHERE id = ?',
            (segment_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_popular_segments(self, limit=10) -> List[Tuple[int, str, int]]:
        """
        Get most frequently visited segments
        
        Args:
            limit: Maximum number of segments to retrieve
            
        Returns:
            List of (segment_id, segment_name, effort_count) tuples
        """
        cursor = self.conn.execute(
            '''
            SELECT s.id, s.name, COUNT(*) as effort_count
            FROM segment_efforts se
            JOIN segments s ON se.segment_id = s.id
            GROUP BY s.id
            ORDER BY effort_count DESC
            LIMIT ?
            ''',
            (limit,)
        )
        return [(row['id'], row['name'], row['effort_count']) for row in cursor.fetchall()]
    
    def get_best_efforts_by_segment(self, segment_id: int, limit=1) -> List[Dict]:
        """
        Get best efforts for a specific segment
        
        Args:
            segment_id: Strava segment ID
            limit: Maximum number of efforts to retrieve
            
        Returns:
            List of segment efforts
        """
        cursor = self.conn.execute(
            '''
            SELECT se.*, a.name as activity_name, a.type as activity_type 
            FROM segment_efforts se
            JOIN activities a ON se.activity_id = a.id
            WHERE se.segment_id = ?
            ORDER BY se.elapsed_time ASC
            LIMIT ?
            ''',
            (segment_id, limit)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def get_segments_by_recent_activity(self, days: int = 30, limit: int = 10) -> List[Tuple[int, str, str, str]]:
        """
        Get segments that have been active in the recent time period
        
        Args:
            days: Number of days to look back
            limit: Maximum number of segments to retrieve
            
        Returns:
            List of (segment_id, segment_name, last_activity_date, activity_name) tuples
        """
        # Calculate the cutoff date
        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        cursor = self.conn.execute(
            '''
            SELECT 
                s.id as segment_id, 
                s.name as segment_name, 
                MAX(se.start_date) as last_activity_date,
                a.name as activity_name
            FROM segment_efforts se
            JOIN segments s ON se.segment_id = s.id
            JOIN activities a ON se.activity_id = a.id
            WHERE se.start_date > ?
            GROUP BY s.id
            ORDER BY last_activity_date DESC
            LIMIT ?
            ''',
            (cutoff_date, limit)
        )
        
        return [(
            row['segment_id'], 
            row['segment_name'],
            row['last_activity_date'],
            row['activity_name']
        ) for row in cursor.fetchall()]
    
    def close(self):
        """Close the database connection"""
        self.conn.close()

# Usage example
if __name__ == "__main__":
    db = SegmentDatabase()
    popular_segments = db.get_popular_segments(5)
    print(f"Found {len(popular_segments)} popular segments")
    
    for segment_id, name, count in popular_segments:
        print(f"Segment: {name} ({count} efforts)")
        best_effort = db.get_best_efforts_by_segment(segment_id)
        if best_effort:
            print(f"  Best time: {best_effort[0]['elapsed_time']} seconds")
    
    db.close()
