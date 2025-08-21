#!/usr/bin/env python3
import sys
import gzip
import tempfile
import os
from pprint import pprint

try:
    import fitparse
    HAS_FITPARSE = True
except ImportError:
    HAS_FITPARSE = False
    print("Error: fitparse library not installed. Run 'pip install fitparse' first.")
    sys.exit(1)

def analyze_fit_file(file_path):
    """
    Analyze a FIT file to find segment efforts and structure
    """
    print(f"Analyzing FIT file: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return
    
    # Create a temp file to extract the gzipped content if needed
    temp_fit_path = None
    
    try:
        if file_path.endswith('.gz'):
            print("Extracting gzipped FIT file...")
            with tempfile.NamedTemporaryFile(delete=False) as temp_fit:
                # Extract the gzipped content
                with gzip.open(file_path, 'rb') as gz_file:
                    temp_fit.write(gz_file.read())
                    temp_fit_path = temp_fit.name
            fit_path = temp_fit_path
        else:
            fit_path = file_path
        
        print("Parsing FIT file...")
        fitfile = fitparse.FitFile(fit_path)
        
        # Get all message types in the file
        message_types = set()
        for record in fitfile.get_messages():
            message_types.add(record.name)
        
        print(f"\nMessage types in file: {sorted(message_types)}")
        
        # Check for segment-related messages
        segment_related = ['segment_lap', 'segment_point', 'segment_id', 'lap', 'segment']
        segment_messages = [msg for msg in segment_related if msg in message_types]
        
        print(f"\nSegment-related message types: {segment_messages}")
        
        # Analyze each segment-related message type
        for message_type in segment_messages:
            print(f"\n=== {message_type.upper()} Messages ===")
            
            # Count messages of this type
            messages = list(fitfile.get_messages(message_type))
            print(f"Count: {len(messages)}")
            
            if not messages:
                continue
                
            # Get field names from the first message
            first_message = messages[0]
            field_names = [field.name for field in first_message.fields]
            print(f"Fields: {sorted(field_names)}")
            
            # Print sample of messages
            print("\nSample data:")
            for i, message in enumerate(messages[:3]):  # Show up to 3 samples
                print(f"\n  Message {i+1}:")
                for field in message.fields:
                    if field.value is not None:
                        print(f"    {field.name}: {field.value}")
        
        # Look specifically for segment_id values
        print("\n=== SEGMENT IDs ===")
        segment_ids_found = False
        
        for message_type in segment_messages:
            for message in fitfile.get_messages(message_type):
                for field in message.fields:
                    if field.name == "segment_id" and field.value is not None:
                        segment_ids_found = True
                        print(f"Found segment_id: {field.value} in {message_type}")
        
        if not segment_ids_found:
            print("No segment IDs found in this file.")
            
            # Check if there are any fields containing 'segment'
            print("\n=== LOOKING FOR ANY 'SEGMENT' RELATED FIELDS ===")
            segment_fields_found = False
            
            for record in fitfile.get_messages():
                for field in record.fields:
                    if 'segment' in field.name.lower() and field.value is not None:
                        segment_fields_found = True
                        print(f"Found in {record.name}: {field.name} = {field.value}")
            
            if not segment_fields_found:
                print("No segment-related fields found.")
    
    except Exception as e:
        print(f"Error analyzing FIT file: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up temp file
        if temp_fit_path and os.path.exists(temp_fit_path):
            os.unlink(temp_fit_path)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analyze_fit_file.py <path_to_fit_file>")
        sys.exit(1)
        
    analyze_fit_file(sys.argv[1])
