import sqlite3
import os
import pathlib

DB_FILE = pathlib.Path(os.path.join('db', 'infernece.db'))

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute("SELECT * FROM images_status")
rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()