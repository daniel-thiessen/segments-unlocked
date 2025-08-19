from src.storage import SegmentDatabase

db = SegmentDatabase()
cursor = db.conn.execute('SELECT id, name FROM segments LIMIT 5')
rows = cursor.fetchall()
print("Available segments:")
for row in rows:
    print(f'Segment ID: {row[0]}, Name: {row[1]}')
db.close()
