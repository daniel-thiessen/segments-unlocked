#!/usr/bin/env python3
import os
import sys
import gzip
import tempfile
import json
from pprint import pprint

try:
    import fitparse
    HAS_FITPARSE = True
except ImportError:
    HAS_FITPARSE = False
    print("Error: fitparse library not installed. Run 'pip install fitparse' first.")
    sys.exit(1)

def examine_fit_file(fit_file):
    """Examine a single FIT file in detail to look for segment efforts"""
    print(f"Examining FIT file: {fit_file}")
    
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
        
        # Parse the FIT file
        fitfile = fitparse.FitFile(fit_path)
        
        # Get message types
        message_types = {}
        lap_count = 0
        
        # First pass: collect message types and counts
        for message in fitfile.get_messages():
            msg_type = message.name
            if msg_type not in message_types:
                message_types[msg_type] = 0
            message_types[msg_type] += 1
            
            if msg_type == 'lap':
                lap_count += 1
        
        # Print message types and counts
        print("\nMessage Types:")
        for msg_type, count in message_types.items():
            print(f"  {msg_type}: {count} messages")
            
        # Look for segment laps specifically
        print(f"\nFound {lap_count} laps in total")
        
        # Examine lap messages in detail
        print("\nLap Details:")
        for i, message in enumerate(fitfile.get_messages('lap')):
            lap_data = {}
            has_segment_info = False
            
            # Extract fields
            for field in message:
                field_name = field.name
                field_value = field.value
                lap_data[field_name] = field_value
                
                # Check for segment-related fields
                if 'segment' in field_name.lower() or (field_name == 'name' and field_value is not None):
                    has_segment_info = True
            
            # Only print laps that might be segments
            if has_segment_info:
                print(f"\nLap {i+1}:")
                
                # Show selected important fields first
                important_fields = ['name', 'start_time', 'total_elapsed_time', 'total_distance', 'segment_id', 'segment_name']
                for field in important_fields:
                    if field in lap_data:
                        print(f"  {field}: {lap_data[field]}")
                
                # Then print remaining fields
                print("  Other fields:")
                for field, value in lap_data.items():
                    if field not in important_fields:
                        print(f"    {field}: {value}")
            
        # Look for any segment_lap messages
        print("\nLooking for segment_lap messages:")
        segment_lap_found = False
        for message in fitfile.get_messages('segment_lap'):
            segment_lap_found = True
            print("  Found segment_lap message:")
            for field in message:
                print(f"    {field.name}: {field.value}")
                
        if not segment_lap_found:
            print("  No segment_lap messages found.")
            
        # Check for any fields containing 'segment' in any message type
        print("\nChecking for fields containing 'segment' in any message type:")
        segment_fields_found = False
        for message in fitfile.get_messages():
            for field in message:
                if 'segment' in field.name.lower() and field.value is not None:
                    segment_fields_found = True
                    print(f"  Found in {message.name}: {field.name} = {field.value}")
                    
        if not segment_fields_found:
            print("  No segment-related fields found in any message.")
            
    except Exception as e:
        print(f"Error examining {fit_file}: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Clean up temp file
        if temp_fit_path and os.path.exists(temp_fit_path):
            try:
                os.unlink(temp_fit_path)
            except:
                pass

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python examine_fit_file.py <path_to_fit_file>")
        sys.exit(1)
    
    fit_file = sys.argv[1]
    examine_fit_file(fit_file)
