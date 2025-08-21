#!/usr/bin/env python3
import csv
import sys

def analyze_segments_csv(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Get the field names
            fields = reader.fieldnames
            print(f"Field names: {fields}")
            
            # Count rows
            rows = list(reader)
            print(f"Total rows: {len(rows)}")
            
            # Print a few sample rows
            print("\nSample rows:")
            for i in range(min(5, len(rows))):
                print(rows[i])
                
    except Exception as e:
        print(f"Error: {e}")
        
if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "/Users/daniel/Downloads/strava_archive_extract/segments.csv"
        
    analyze_segments_csv(file_path)
