import os
import json
import logging
import zipfile
import csv
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import glob

from src.storage import SegmentDatabase
from src.data_retrieval import get_segment_details, get_activity_details, get_segment_efforts

# Set up logging
logger = logging.getLogger(__name__)

class ArchiveImporter:
    """Imports data from a Strava data export archive"""
    
    def __init__(self, db: SegmentDatabase):
        """
        Initialize the archive importer
        
        Args:
            db: Database connection
        """
        self.db = db
    
    def import_from_zip(self, zip_path: str, extract_dir: Optional[str] = None, fetch_segments: bool = False) -> Tuple[int, int, int]:
        """
        Import data from a Strava zip archive
        
        Args:
            zip_path: Path to the Strava archive zip file
            extract_dir: Directory to extract the archive to (if None, will extract to a temp directory)
            fetch_segments: Whether to fetch segment efforts from API (can be rate-limited)
            
        Returns:
            Tuple of (activities_count, segment_efforts_count, segments_count)
        """
        logger.info(f"Importing data from archive: {zip_path}")
        
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"Archive file not found: {zip_path}")
            
        if extract_dir is None:
            extract_dir = os.path.join(os.path.dirname(zip_path), "strava_archive_extract")
        
        os.makedirs(extract_dir, exist_ok=True)
        
        # Extract the archive
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            logger.info(f"Extracting archive to {extract_dir}")
            zip_ref.extractall(extract_dir)
        
        # Process the extracted files
        return self.import_from_directory(extract_dir, fetch_segments)
    
    def import_from_directory(self, directory: str, fetch_segments: bool = False) -> Tuple[int, int, int]:
        """
        Import data from an extracted Strava archive directory
        
        Args:
            directory: Path to the directory containing the extracted Strava archive
            fetch_segments: Whether to fetch segment efforts from API (can be rate-limited)
            
        Returns:
            Tuple of (activities_count, segment_efforts_count, segments_count)
        """
        logger.info(f"Importing data from directory: {directory}")
        
        activities_dir = os.path.join(directory, "activities")
        if not os.path.exists(activities_dir):
            activities_dir = directory  # Try the root directory
            
        # First try to find activity JSON files (some exports might have these)
        activity_files = glob.glob(os.path.join(activities_dir, "**", "*.json"), recursive=True)
        
        activities_count = 0
        segment_efforts_count = 0
        segments = set()  # Track unique segments
        
        # If JSON files exist, process them
        if activity_files:
            logger.info(f"Found {len(activity_files)} JSON activity files")
            
            # Process each activity file
            for activity_file in activity_files:
                try:
                    with open(activity_file, 'r', encoding='utf-8') as f:
                        activity_data = json.load(f)
                    
                    # Check if this is a valid activity
                    if not isinstance(activity_data, dict) or 'id' not in activity_data:
                        logger.warning(f"Invalid activity data in {activity_file}")
                        continue
                    
                    # Save the activity
                    self.db.save_activity(activity_data)
                    activities_count += 1
                    
                    # Process segment efforts if available
                    segment_efforts = activity_data.get('segment_efforts', [])
                    segment_efforts_count += self._process_segment_efforts(segment_efforts, activity_data['id'], segments)
                        
                except Exception as e:
                    logger.error(f"Error processing activity file {activity_file}: {e}")
        
        # If no JSON files or looking for more data, try CSV
        csv_file = os.path.join(directory, "activities.csv")
        if os.path.exists(csv_file):
            logger.info(f"Processing activities CSV file: {csv_file}")
            
            try:
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    csv_activities = list(reader)
                
                logger.info(f"Found {len(csv_activities)} activities in CSV")
                
                # Process each activity row
                for row in csv_activities:
                    try:
                        # Extract activity ID and convert to integer
                        activity_id = int(row.get('Activity ID', 0))
                        if activity_id == 0:
                            continue
                        
                        # Build activity data from CSV
                        activity_data = self._build_activity_from_csv(row)
                        
                        # Skip if we don't have basic required data
                        if not activity_data:
                            continue
                        
                        # Save the activity
                        self.db.save_activity(activity_data)
                        activities_count += 1
                        
                        # Only fetch segment efforts if explicitly requested
                        if fetch_segments:
                            # Try to fetch segment efforts from API if needed (will respect rate limits)
                            efforts_count, segment_count = self._fetch_segment_efforts(activity_id, segments)
                            segment_efforts_count += efforts_count
                        
                    except Exception as e:
                        logger.warning(f"Error processing activity {row.get('Activity ID', 'unknown')}: {e}")
            except Exception as e:
                logger.error(f"Error processing activities CSV file: {e}")
        
        logger.info(f"Imported {activities_count} activities, {segment_efforts_count} segment efforts, {len(segments)} segments")
        return activities_count, segment_efforts_count, len(segments)
    
    def _build_activity_from_csv(self, row: Dict[str, str]) -> Dict:
        """
        Convert a CSV row to an activity dictionary
        
        Args:
            row: CSV row as dictionary
            
        Returns:
            Activity data dictionary
        """
        # Extract the activity ID
        try:
            activity_id = int(row.get('Activity ID', 0))
            if activity_id == 0:
                return {}
        except (ValueError, TypeError):
            return {}
        
        # Parse date string
        date_str = row.get('Activity Date', '')
        try:
            # Try to parse the date - adjust format as needed
            date_match = re.search(r'([A-Za-z]+ \d+, \d{4}), (\d+:\d+:\d+ [AP]M)', date_str)
            if date_match:
                date_part = date_match.group(1)
                time_part = date_match.group(2)
                # Format: "Jun 3, 2013, 11:56:31 PM" -> "2013-06-03T23:56:31Z"
                dt = datetime.strptime(f"{date_part} {time_part}", "%b %d, %Y %I:%M:%S %p")
                start_date = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                # Try alternative formats if needed
                start_date = date_str
        except Exception:
            # If date parsing fails, use empty string
            start_date = date_str
            
        # Helper function to safely convert values
        def safe_float(value, default=0.0):
            if not value or value == '':
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default
                
        def safe_int(value, default=0):
            if not value or value == '':
                return default
            try:
                # First convert to float to handle values like "1234.0"
                return int(float(value))
            except (ValueError, TypeError):
                return default
        
        # Build the activity object with data from CSV
        activity = {
            'id': activity_id,
            'name': row.get('Activity Name', ''),
            'type': row.get('Activity Type', ''),
            'start_date': start_date,
            'distance': safe_float(row.get('Distance', 0)) * 1000,  # Convert to meters
            'moving_time': safe_int(row.get('Moving Time', 0)),
            'elapsed_time': safe_int(row.get('Elapsed Time', 0)),
            'total_elevation_gain': safe_float(row.get('Elevation Gain', 0)),
            'average_speed': safe_float(row.get('Average Speed', 0)),
            'max_speed': safe_float(row.get('Max Speed', 0)),
            'average_watts': safe_float(row.get('Average Watts', 0)) if row.get('Average Watts') else None,
            'kilojoules': safe_float(row.get('Total Work', 0)) if row.get('Total Work') else None,
            'device_watts': 1 if row.get('Average Watts') else 0,
            'has_heartrate': 1 if row.get('Average Heart Rate') else 0,
            'average_heartrate': safe_float(row.get('Average Heart Rate', 0)) if row.get('Average Heart Rate') else None,
            'max_heartrate': safe_float(row.get('Max Heart Rate', 0)) if row.get('Max Heart Rate') else None,
        }
        
        # Filter out None values
        return {k: v for k, v in activity.items() if v is not None}
    
    def _process_segment_efforts(self, efforts: List[Dict], activity_id: int, segments: set) -> int:
        """
        Process segment efforts and save them to the database
        
        Args:
            efforts: List of segment effort dictionaries
            activity_id: ID of the parent activity
            segments: Set to track unique segments
            
        Returns:
            Number of segment efforts processed
        """
        count = 0
        for effort in efforts:
            if not isinstance(effort, dict) or 'id' not in effort:
                continue
                
            # Add activity ID if not present (needed for DB relations)
            if 'activity_id' not in effort:
                effort['activity_id'] = activity_id
                
            # Save the segment effort
            self.db.save_segment_effort(effort)
            count += 1
            
            # Process segment data
            segment = effort.get('segment')
            if isinstance(segment, dict) and 'id' in segment:
                # Store segment ID to count unique segments
                segments.add(segment['id'])
                
                # Save segment data
                self.db.save_segment(segment)
        
        return count
    
    def _fetch_segment_efforts(self, activity_id: int, segments: set) -> Tuple[int, int]:
        """
        Fetch segment efforts for an activity from the API
        
        Args:
            activity_id: Activity ID
            segments: Set to track unique segments
            
        Returns:
            Tuple of (efforts_count, new_segments_count)
        """
        try:
            # First, check if we already have efforts for this activity in the database
            cursor = self.db.conn.execute(
                "SELECT COUNT(*) as count FROM segment_efforts WHERE activity_id = ?",
                (activity_id,)
            )
            row = cursor.fetchone()
            if row and row['count'] > 0:
                # Already have efforts for this activity
                return 0, 0
            
            # Fetch segment efforts from API (this respects rate limits)
            efforts = get_segment_efforts(activity_id)
            
            # Get original segments count
            original_segment_count = len(segments)
            
            # Process and save the efforts
            efforts_count = self._process_segment_efforts(efforts, activity_id, segments)
            
            # Calculate new segments added
            new_segments_count = len(segments) - original_segment_count
            
            return efforts_count, new_segments_count
            
        except Exception as e:
            logger.warning(f"Error fetching segment efforts for activity {activity_id}: {e}")
            return 0, 0
    
    def fetch_missing_segment_details(self) -> int:
        """
        Fetch additional details for segments that were imported from the archive
        
        Some segment data in the activities export might be incomplete.
        This method queries the Strava API to get full segment details.
        Note: This method is rate-limited and may take a long time for many segments.
        
        Returns:
            Number of segments updated with additional details
        """
        with self.db.conn:
            # Get all segments that might have incomplete data
            cursor = self.db.conn.execute(
                """
                SELECT id, name 
                FROM segments 
                WHERE coordinate_points IS NULL OR raw_data = '{}'
                """
            )
            segments = cursor.fetchall()
            
        logger.info(f"Found {len(segments)} segments with potentially incomplete data")
        
        updated_count = 0
        
        for segment in segments:
            segment_id = segment['id']
            try:
                logger.info(f"Fetching details for segment {segment_id} ({segment['name']})")
                segment_detail = get_segment_details(segment_id)
                self.db.save_segment(segment_detail)
                updated_count += 1
            except Exception as e:
                logger.warning(f"Could not fetch details for segment {segment_id}: {e}")
        
        logger.info(f"Updated {updated_count} segments with additional details")
        return updated_count

if __name__ == "__main__":
    # Test the archive importer
    logging.basicConfig(level=logging.INFO)
    
    import sys
    if len(sys.argv) < 2:
        print("Usage: python archive_import.py <path_to_strava_archive.zip>")
        sys.exit(1)
        
    db = SegmentDatabase()
    importer = ArchiveImporter(db)
    
    try:
        archive_path = sys.argv[1]
        if archive_path.lower().endswith('.zip'):
            activities, efforts, segments = importer.import_from_zip(archive_path)
        else:
            activities, efforts, segments = importer.import_from_directory(archive_path)
            
        print(f"Successfully imported {activities} activities with {efforts} segment efforts across {segments} unique segments")
        
        # Ask if user wants to fetch missing segment details
        response = input("Do you want to fetch additional segment details from Strava API? This may take a while. (y/N): ")
        if response.lower() == 'y':
            updated = importer.fetch_missing_segment_details()
            print(f"Updated {updated} segments with additional details")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()
