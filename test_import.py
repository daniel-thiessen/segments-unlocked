from src.storage import SegmentDatabase

# Connect to the database
db = SegmentDatabase()

# Get the latest activities
activities = db.get_latest_activities(5)
print(f'Found {len(activities)} recent activities')

# Display activity details
for activity in activities:
    print(f"  {activity['start_date']} - {activity['name']} ({activity['type']})")

# Get popular segments
popular_segments = db.get_popular_segments(5)
print(f"\nFound {len(popular_segments)} popular segments")

# Close the database connection
db.close()
