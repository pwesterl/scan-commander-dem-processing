import os
import sqlite3
import pathlib

DB_FILE = pathlib.Path(os.path.join('db', 'infernece.db'))
IMAGE_DIR = pathlib.Path('test_images')   
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff") # kör på endast tiff.?
STATUS_MAP = {
    '0' : 'unprocessed',
    '1' : 'in_progress',
    '2' : 'processed', 
    '3' : 'integrated',
    '4' : 'failed'
}

def create_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS images_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_name TEXT NOT NULL,
            status TEXT DEFAULT '',
            comment TEXT DEFAULT ''
        )
    ''')    
    conn.commit()
    conn.close()

def insert_images_from_directory():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    for filename in os.listdir(IMAGE_DIR):
        if filename.lower().endswith(IMAGE_EXTENSIONS):
            cursor.execute('''
                INSERT INTO images_status (image_name, status, comment)
                VALUES (?, ?, ?)
            ''', (filename, STATUS_MAP['0'], ''))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_database()
    insert_images_from_directory()
    print("Image files inserted into database successfully.")