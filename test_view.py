from src.visualization import SegmentVisualizer
from src.analysis import SegmentAnalyzer
from src.storage import SegmentDatabase
import sqlite3
import os

# Initialize components
db = SegmentDatabase()
analyzer = SegmentAnalyzer(db)
visualizer = SegmentVisualizer(db, analyzer)

# Check for segments in the database
conn = sqlite3.connect('./data/segments.db')
cursor = conn.execute('SELECT id, name FROM segments LIMIT 5')
segments = cursor.fetchall()

print("Available segments:")
for segment in segments:
    print(f'Segment ID: {segment[0]}, Name: {segment[1]}')

# Let's use the first segment ID we find
if segments:
    segment_id = segments[0][0]
    segment_name = segments[0][1]
    print(f"\nCreating dashboard for segment: {segment_name} (ID: {segment_id})")
    visualizer.create_segment_dashboard(segment_id)
    print(f"Dashboard created in the output directory")
else:
    print("No segments found in the database")

# Also test with a specific activity if available
cursor = conn.execute('SELECT id, name FROM activities LIMIT 1')
activity = cursor.fetchone()

if activity:
    activity_id = activity[0]
    activity_name = activity[1]
    print(f"\nCreating dashboard for activity: {activity_name} (ID: {activity_id})")
    visualizer.create_activity_segments_dashboard(activity_id)
    print(f"Dashboard created in the output directory")
else:
    print("No activities found in the database")

conn.close()
db.close()
