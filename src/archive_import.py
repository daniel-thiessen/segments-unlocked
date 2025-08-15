import os
import json
import logging
import zipfile
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import glob

from src.storage import SegmentDatabase
from src.data_retrieval import get_segment_details

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
    
    def import_from_zip(self, zip_path: str, extract_dir: Optional[str] = None) -> Tuple[int, int, int]:
        """
        Import data from a Strava zip archive
        
        Args:
            zip_path: Path to the Strava archive zip file
            extract_dir: Directory to extract the archive to (if None, will extract to a temp directory)
            
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
        return self.import_from_directory(extract_dir)
    
    def import_from_directory(self, directory: str) -> Tuple[int, int, int]:
        """
        Import data from an extracted Strava archive directory
        
        Args:
            directory: Path to the directory containing the extracted Strava archive
            
        Returns:
            Tuple of (activities_count, segment_efforts_count, segments_count)
        """
        logger.info(f"Importing data from directory: {directory}")
        
        activities_dir = os.path.join(directory, "activities")
        if not os.path.exists(activities_dir):
            activities_dir = directory  # Try the root directory
            
        # Find all activity JSON files
        activity_files = glob.glob(os.path.join(activities_dir, "**", "*.json"), recursive=True)
        
        if not activity_files:
            logger.warning(f"No activity JSON files found in {activities_dir}")
            return 0, 0, 0
            
        logger.info(f"Found {len(activity_files)} activity files")
        
        activities_count = 0
        segment_efforts_count = 0
        segments = set()  # Track unique segments
        
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
                for effort in segment_efforts:
                    if not isinstance(effort, dict) or 'id' not in effort:
                        continue
                        
                    # Add activity ID if not present (needed for DB relations)
                    if 'activity_id' not in effort:
                        effort['activity_id'] = activity_data['id']
                        
                    # Save the segment effort
                    self.db.save_segment_effort(effort)
                    segment_efforts_count += 1
                    
                    # Process segment data
                    segment = effort.get('segment')
                    if isinstance(segment, dict) and 'id' in segment:
                        # Store segment ID to count unique segments
                        segments.add(segment['id'])
                        
                        # Save segment data
                        self.db.save_segment(segment)
                        
            except Exception as e:
                logger.error(f"Error processing activity file {activity_file}: {e}")
        
        logger.info(f"Imported {activities_count} activities, {segment_efforts_count} segment efforts, {len(segments)} segments")
        return activities_count, segment_efforts_count, len(segments)
    
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
