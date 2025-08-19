#!/usr/bin/env python3
from src.storage import SegmentDatabase

def main():
    db = SegmentDatabase()
    db.conn.row_factory = lambda cursor, row: {
        column[0]: row[idx] for idx, column in enumerate(cursor.description)
    }
    
    # Check schema
    print("Checking database schema...")
    cursor = db.conn.execute("PRAGMA table_info(segments)")
    columns = cursor.fetchall()
    for col in columns:
        print(f"Column: {col['name']}, Type: {col['type']}")
    
    print("Available segments:")
    cursor = db.conn.execute('SELECT id, name FROM segments LIMIT 5')
    rows = cursor.fetchall()
    for row in rows:
        print(f'Segment ID: {row["id"]}, Name: {row["name"]}')

    # Check segment elevation data
    print("\nChecking segment elevation data...")
    cursor = db.conn.execute("""
        SELECT id, name, elevation_low, elevation_high, 
               average_grade, maximum_grade
        FROM segments
        LIMIT 5
    """)
    
    rows = cursor.fetchall()
    for row in rows:
        print(f"\nSegment ID: {row['id']}")
        print(f"Name: {row['name']}")
        print(f"Elevation Low: {row.get('elevation_low')}")
        print(f"Elevation High: {row.get('elevation_high')}")
        elevation_gain = row.get('elevation_high', 0) - row.get('elevation_low', 0) if row.get('elevation_high') is not None and row.get('elevation_low') is not None else 0
        print(f"Calculated Elevation Gain: {elevation_gain}")
        print(f"Average Grade: {row.get('average_grade')}%")
        print(f"Maximum Grade: {row.get('maximum_grade')}%")
    
    # Check if we have any segments with non-zero elevation gain
    cursor = db.conn.execute("""
        SELECT COUNT(*) as count FROM segments 
        WHERE elevation_high IS NOT NULL AND elevation_low IS NOT NULL
        AND elevation_high > elevation_low
    """)
    
    count = cursor.fetchone()["count"]
    print(f"\nSegments with non-zero elevation gain: {count}")
    
    if count > 0:
        cursor = db.conn.execute("""
            SELECT id, name, elevation_low, elevation_high
            FROM segments 
            WHERE elevation_high IS NOT NULL AND elevation_low IS NOT NULL
            AND elevation_high > elevation_low
            LIMIT 3
        """)
        
        print("\nExamples of segments with elevation gain:")
        rows = cursor.fetchall()
        for row in rows:
            print(f"Segment ID: {row['id']}")
            print(f"Name: {row['name']}")
            print(f"Elevation Low: {row.get('elevation_low')}")
            print(f"Elevation High: {row.get('elevation_high')}")
            elevation_gain = row.get('elevation_high', 0) - row.get('elevation_low', 0) if row.get('elevation_high') is not None and row.get('elevation_low') is not None else 0
            print(f"Calculated Elevation Gain: {elevation_gain}")
    
    db.close()

if __name__ == "__main__":
    main()
