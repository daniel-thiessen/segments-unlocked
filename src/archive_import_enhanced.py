import os
import json
import logging
import zipfile
import csv
import re
import gzip
import tempfile
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import glob

# Import fitparse for handling FIT files
try:
    import fitparse
    HAS_FITPARSE = True
except ImportError:
    HAS_FITPARSE = False

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
            fetch_segments: Whether to fetch segment details from API (can be rate-limited)
            
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
            fetch_segments: Whether to fetch segment details from API (can be rate-limited)
            
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
        segments: set[int] = set()  # Track unique segments
        
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
        
        # Process FIT.GZ files if present and fitparse is available
        if HAS_FITPARSE:
            fit_files = glob.glob(os.path.join(activities_dir, "**", "*.fit.gz"), recursive=True)
            if fit_files:
                logger.info(f"Found {len(fit_files)} FIT.GZ activity files")
                
                # Process activities.csv first to get activity metadata
                csv_file = os.path.join(directory, "activities.csv")
                activity_map = {}
                
                if os.path.exists(csv_file):
                    logger.info(f"Processing activities CSV file to get activity metadata: {csv_file}")
                    try:
                        with open(csv_file, 'r', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                activity_id = int(row.get('Activity ID', 0))
                                if activity_id > 0:
                                    activity_map[activity_id] = row
                    except Exception as e:
                        logger.error(f"Error reading activities.csv: {e}")
                
                # Process each FIT file
                for fit_file in fit_files:
                    try:
                        # Extract activity ID from filename
                        file_basename = os.path.basename(fit_file)
                        activity_id = int(os.path.splitext(os.path.splitext(file_basename)[0])[0])
                        
                        # Check if we already have activity data
                        activity_data = None
                        if activity_id in activity_map:
                            activity_data = self._build_activity_from_csv(activity_map[activity_id])
                        
                        if not activity_data:
                            activity_data = {'id': activity_id}
                        
                        # Save the basic activity data
                        self.db.save_activity(activity_data)
                        activities_count += 1
                        
                        # Extract segment efforts from FIT file
                        segment_efforts = self._extract_segment_efforts_from_fit(fit_file, activity_id)
                        if segment_efforts:
                            segment_efforts_count += self._process_segment_efforts(segment_efforts, activity_id, segments)
                    except Exception as e:
                        logger.error(f"Error processing FIT file {fit_file}: {e}")
            
        # If no JSON or FIT files or looking for more data, try CSV
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
                        
                        # Only fetch segment efforts from API if explicitly requested
                        # and we don't already have segment efforts for this activity
                        if fetch_segments:
                            # Check if we already have efforts for this activity
                            cursor = self.db.conn.execute(
                                "SELECT COUNT(*) as count FROM segment_efforts WHERE activity_id = ?",
                                (activity_id,)
                            )
                            row = cursor.fetchone()
                            if row and row['count'] == 0:
                                # Try to fetch segment efforts from API (will respect rate limits)
                                efforts_count, segment_count = self._fetch_segment_efforts(activity_id, segments)
                                segment_efforts_count += efforts_count
                        
                    except Exception as e:
                        logger.warning(f"Error processing activity {row.get('Activity ID', 'unknown')}: {e}")
            except Exception as e:
                logger.error(f"Error processing activities CSV file: {e}")
        
        # Process segments.csv if available
        segments_csv = os.path.join(directory, "segments.csv")
        if os.path.exists(segments_csv):
            logger.info(f"Processing segments CSV file: {segments_csv}")
            try:
                with open(segments_csv, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            segment_id = int(row.get('Segment ID', 0))
                            if segment_id == 0:
                                continue
                                
                            # Save basic segment data
                            segment_data = self._build_segment_from_csv(row)
                            if segment_data:
                                self.db.save_segment(segment_data)
                                segments.add(segment_id)
                        except Exception as e:
                            logger.warning(f"Error processing segment {row.get('Segment ID', 'unknown')}: {e}")
            except Exception as e:
                logger.error(f"Error processing segments CSV file: {e}")
                
        logger.info(f"Imported {activities_count} activities, {segment_efforts_count} segment efforts, {len(segments)} segments")
        return activities_count, segment_efforts_count, len(segments)
    
    def _extract_segment_efforts_from_fit(self, fit_file_path: str, activity_id: int) -> List[Dict]:
        """
        Extract segment efforts from a FIT file
        
        Args:
            fit_file_path: Path to the FIT.GZ file
            activity_id: ID of the activity
            
        Returns:
            List of segment effort dictionaries
        """
        if not HAS_FITPARSE:
            logger.warning("fitparse library not available, cannot process FIT files")
            return []
            
        segment_efforts = []
        temp_fit_path = None
        
        try:
            # Create a temp file to extract the gzipped content
            with tempfile.NamedTemporaryFile(delete=False) as temp_fit:
                # Extract the gzipped content
                with gzip.open(fit_file_path, 'rb') as gz_file:
                    temp_fit.write(gz_file.read())
                    temp_fit_path = temp_fit.name
            
            # Import the module again here to ensure it's loaded
            import fitparse
            
            # Parse the FIT file
            fitfile = fitparse.FitFile(temp_fit_path)
            
            # Look for segment data in the file
            try:
                # Some FIT files might not have segment data or use a different message type
                # Handle possible variations in the FIT file structure
                for message in fitfile.get_messages(['segment_lap', 'lap']):
                    try:
                        # Extract data from message fields
                        segment_data = {}
                        # segment_id can be int or str depending on FIT file content
                        segment_id: Optional[int | str] = None
                        
                        # Convert the fields to a dictionary
                        for field in message.fields:
                            if field.name == 'segment_id' and field.value is not None:
                                try:
                                    segment_id = int(field.value)
                                except (ValueError, TypeError):
                                    # Some FIT files might have non-integer segment IDs
                                    segment_id = str(field.value)
                            
                            # Add all fields to the segment data
                            if field.value is not None:
                                segment_data[field.name] = field.value
                        
                        # If we found a segment ID
                        if segment_id:
                            # Create a unique effort ID based on activity and segment
                            effort_id_str = f"{activity_id}{segment_id}"
                            # Limit the length to avoid integer overflow
                            if len(effort_id_str) > 9:
                                effort_id_str = effort_id_str[:9]
                                
                            try:
                                effort_id = int(effort_id_str)
                            except ValueError:
                                # Fallback if conversion fails
                                import hashlib
                                effort_id = int(hashlib.md5(effort_id_str.encode()).hexdigest(), 16) % 10**9
                                
                            # Create a simplified segment effort structure
                            effort = {
                                'id': effort_id,
                                'activity_id': activity_id,
                                'segment_id': segment_id,
                                'name': segment_data.get('name', f"Segment {segment_id}"),
                                'elapsed_time': segment_data.get('total_elapsed_time', 0),
                                'moving_time': segment_data.get('total_timer_time', 0),
                                'start_date': segment_data.get('start_time', None),
                                'distance': segment_data.get('total_distance', 0),
                                'average_watts': segment_data.get('avg_power', None),
                                'device_watts': 1 if segment_data.get('avg_power') else 0,
                                'average_heartrate': segment_data.get('avg_heart_rate', None),
                                'max_heartrate': segment_data.get('max_heart_rate', None),
                                'pr_rank': 0,  # We don't know this from the FIT file
                                'segment': {
                                    'id': segment_id,
                                    'name': segment_data.get('name', f"Segment {segment_id}"),
                                    # Add more segment details as needed
                                }
                            }
                            
                            segment_efforts.append(effort)
                            
                    except Exception as e:
                        logger.warning(f"Error processing message in FIT file: {e}")
            except Exception as e:
                logger.warning(f"Error getting messages from FIT file: {e}")
                    
        except Exception as e:
            logger.error(f"Error extracting segment efforts from FIT file {fit_file_path}: {e}")
        finally:
            # Ensure the temporary file is deleted
            if temp_fit_path and os.path.exists(temp_fit_path):
                try:
                    os.unlink(temp_fit_path)
                except Exception as e:
                    logger.warning(f"Error deleting temporary file: {e}")
            
        logger.info(f"Extracted {len(segment_efforts)} segment efforts from FIT file {fit_file_path}")
        return segment_efforts
    
    def _build_segment_from_csv(self, row: Dict[str, str]) -> Dict:
        """
        Convert a CSV row to a segment dictionary
        
        Args:
            row: CSV row as dictionary
            
        Returns:
            Segment data dictionary
        """
        try:
            segment_id = int(row.get('Segment ID', 0))
            if segment_id == 0:
                return {}
                
            # Helper functions to safely convert values
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
                    return int(float(value))
                except (ValueError, TypeError):
                    return default
            
            # Build the segment object with data from CSV
            segment = {
                'id': segment_id,
                'name': row.get('Name', ''),
                'activity_type': row.get('Activity Type', ''),
                'distance': safe_float(row.get('Distance', 0)) * 1000,  # Convert to meters
                'average_grade': safe_float(row.get('Average Grade', 0)),
                'maximum_grade': safe_float(row.get('Maximum Grade', 0)),
                'elevation_high': safe_float(row.get('Highest Elevation', 0)),
                'elevation_low': safe_float(row.get('Lowest Elevation', 0)),
                'start_latlng': None,  # Not available in CSV
                'end_latlng': None,  # Not available in CSV
                'climb_category': safe_int(row.get('Category', 0)),
                'city': row.get('City', ''),
                'state': row.get('State', ''),
                'country': row.get('Country', ''),
                'private': 1 if row.get('Private', '').lower() == 'true' else 0,
                'starred': 1 if row.get('Starred', '').lower() == 'true' else 0,
                'coordinate_points': None,  # Will be filled by backfill_segment_details
            }
            
            return segment
            
        except Exception as e:
            logger.warning(f"Error building segment from CSV: {e}")
            return {}
    
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
        
        # Use a single transaction for better performance and to avoid locking issues
        with self.db.conn:
            try:
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
            except Exception as e:
                logger.error(f"Error processing segment efforts batch: {e}")
                self.db.conn.rollback()
                raise
        
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
