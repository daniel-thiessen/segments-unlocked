#!/usr/bin/env python3
import os
import sys
import gzip
import tempfile
import glob
from pprint import pprint

try:
    import fitparse
    HAS_FITPARSE = True
except ImportError:
    HAS_FITPARSE = False
    print("Error: fitparse library not installed. Run 'pip install fitparse' first.")
    sys.exit(1)

def extract_fit_data(fit_path):
    """Extract data from a FIT file safely"""
    try:
        # Parse the FIT file
        fitfile = fitparse.FitFile(fit_path)
        
        # Get all messages as a dictionary keyed by message type
        messages_by_type = {}
        message_types = set()
        
        # Iterate through all messages
        for message in fitfile.get_messages():
            # Get the message type as a string
            try:
                message_type = str(message.name)
            except:
                try:
                    message_type = str(message.mesg_type.name)
                except:
                    message_type = "unknown"
            
            message_types.add(message_type)
            
            if message_type not in messages_by_type:
                messages_by_type[message_type] = []
                
            # Extract fields safely
            fields = {}
            try:
                for field in message:
                    try:
                        field_name = str(field.name)
                        field_value = field.value
                        fields[field_name] = field_value
                    except:
                        pass
            except:
                # If we can't iterate through fields directly
                try:
                    fields = message.get_values()
                except:
                    pass
                    
            messages_by_type[message_type].append(fields)
        
        return {
            "message_types": message_types,
            "messages": messages_by_type
        }
    
    except Exception as e:
        print(f"Error extracting FIT data: {e}")
        import traceback
        traceback.print_exc()
        return {"message_types": set(), "messages": {}}

def scan_for_segment_data(fit_data):
    """Scan the extracted FIT data for segment-related information"""
    segment_data = []
    
    # Words that might indicate segment-related data
    segment_keywords = ["segment", "strava", "leaderboard", "pr", "effort"]
    
    # Iterate through all message types and their messages
    for msg_type, messages in fit_data["messages"].items():
        for msg_idx, msg in enumerate(messages):
            for field_name, field_value in msg.items():
                # Check if any field contains segment-related keywords
                if any(keyword in str(field_name).lower() for keyword in segment_keywords):
                    segment_data.append({
                        "message_type": msg_type,
                        "message_index": msg_idx,
                        "field": field_name,
                        "value": field_value
                    })
                # Also check string values
                elif isinstance(field_value, str) and any(keyword in field_value.lower() for keyword in segment_keywords):
                    segment_data.append({
                        "message_type": msg_type,
                        "message_index": msg_idx,
                        "field": field_name,
                        "value": field_value
                    })
    
    return segment_data

def scan_fit_files_for_segments(directory, max_files=10):
    """Scan multiple FIT files in a directory looking for segment efforts"""
    print(f"Scanning FIT files in: {directory}")
    
    # Find all FIT files
    fit_files = glob.glob(os.path.join(directory, "**/*.fit*"), recursive=True)
    
    if not fit_files:
        print("No FIT files found in directory.")
        return
        
    print(f"Found {len(fit_files)} FIT files. Scanning first {min(max_files, len(fit_files))}...")
    
    # Track message types across all files
    all_message_types = set()
    files_with_segment_data = []
    
    # Process files
    for i, fit_file in enumerate(fit_files[:max_files]):
        print(f"\n[{i+1}/{min(max_files, len(fit_files))}] Processing: {os.path.basename(fit_file)}")
        
        temp_fit_path = None
        try:
            # Handle gzipped files
            if fit_file.endswith('.gz'):
                with tempfile.NamedTemporaryFile(delete=False, mode="wb") as temp_fit:
                    with gzip.open(fit_file, 'rb') as gz_file:
                        temp_fit.write(gz_file.read())
                    temp_fit_path = temp_fit.name
                fit_path = temp_fit_path
            else:
                fit_path = fit_file
            
            # Extract and scan the FIT file
            fit_data = extract_fit_data(fit_path)
            all_message_types.update(fit_data["message_types"])
            
            # Look for segment data
            segment_data = scan_for_segment_data(fit_data)
            
            if segment_data:
                files_with_segment_data.append(fit_file)
                print(f"  Found {len(segment_data)} segment-related fields in {os.path.basename(fit_file)}")
                
                # Print a sample of segment data
                print("  Sample segment data:")
                for item in segment_data[:5]:  # Show up to 5 samples
                    print(f"    {item['message_type']} - {item['field']}: {item['value']}")
                
                if len(segment_data) > 5:
                    print(f"    ... and {len(segment_data) - 5} more")
                
        except Exception as e:
            print(f"  Error processing {fit_file}: {e}")
            
        finally:
            # Clean up temp file
            if temp_fit_path and os.path.exists(temp_fit_path):
                try:
                    os.unlink(temp_fit_path)
                except:
                    pass
    
    # Summary
    print("\n=== SCAN SUMMARY ===")
    print(f"All message types found across files: {sorted(all_message_types)}")
    print(f"Files with segment data: {len(files_with_segment_data)} out of {min(max_files, len(fit_files))}")
    
    if files_with_segment_data:
        print("\nFiles containing segment data:")
        for file_path in files_with_segment_data:
            print(f"  {os.path.basename(file_path)}")
    else:
        print("\nNo files with segment data found in the scanned files.")
        
    # Suggest next steps
    if len(fit_files) > max_files:
        print(f"\nNote: Only scanned {max_files} of {len(fit_files)} files. Run with a larger max_files value to scan more.")

def scan_strava_export_for_segment_data(export_dir):
    """Scan a Strava export directory for segment data in any format"""
    print(f"Scanning Strava export directory: {export_dir}")
    
    # Check for segments.csv
    segments_csv = os.path.join(export_dir, "segments.csv")
    if os.path.exists(segments_csv):
        print(f"\nFound segments.csv file: {segments_csv}")
        try:
            with open(segments_csv, 'r') as f:
                contents = f.read(1024)  # Read first 1KB for preview
                lines = contents.strip().split("\n")
                print(f"First few lines of segments.csv:")
                for i, line in enumerate(lines[:5]):
                    print(f"  {line}")
                if len(lines) > 5:
                    print(f"  ... and {len(lines) - 5} more lines")
        except Exception as e:
            print(f"Error reading segments.csv: {e}")
    else:
        print("No segments.csv file found.")
    
    # Check for segment_efforts.csv
    segment_efforts_csv = os.path.join(export_dir, "segment_efforts.csv")
    if os.path.exists(segment_efforts_csv):
        print(f"\nFound segment_efforts.csv file: {segment_efforts_csv}")
        try:
            with open(segment_efforts_csv, 'r') as f:
                contents = f.read(1024)  # Read first 1KB for preview
                lines = contents.strip().split("\n")
                print(f"First few lines of segment_efforts.csv:")
                for i, line in enumerate(lines[:5]):
                    print(f"  {line}")
                if len(lines) > 5:
                    print(f"  ... and {len(lines) - 5} more lines")
        except Exception as e:
            print(f"Error reading segment_efforts.csv: {e}")
    else:
        print("\nNo segment_efforts.csv file found.")
    
    # Look for activities directory and scan FIT files
    activities_dir = os.path.join(export_dir, "activities")
    if os.path.exists(activities_dir) and os.path.isdir(activities_dir):
        print(f"\nFound activities directory: {activities_dir}")
        scan_fit_files_for_segments(activities_dir, max_files=5)
    else:
        print("\nNo activities directory found.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python scan_fit_files.py <directory_with_fit_files> [max_files]")
        print("  python scan_fit_files.py --strava-export <strava_export_directory>")
        sys.exit(1)
    
    if sys.argv[1] == "--strava-export":
        if len(sys.argv) < 3:
            print("Error: Missing Strava export directory path.")
            print("Usage: python scan_fit_files.py --strava-export <strava_export_directory>")
            sys.exit(1)
        export_dir = sys.argv[2]
        scan_strava_export_for_segment_data(export_dir)
    else:
        directory = sys.argv[1]
        max_files = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        scan_fit_files_for_segments(directory, max_files)
