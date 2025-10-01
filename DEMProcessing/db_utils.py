import sqlite3
import sys
import os
import logging
from pathlib import Path
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
import argparse


DB_FILE = Path("db/inference.db") # Åndra till en mouuntad db /eller egen container?
IMAGE_DIR = Path("/skog-nas01/scan-data/AW_bearbetning_test/")
IMAGE_EXTENSIONS = (".tif",)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] %(message)s",
    "%Y-%m-%d %H:%M:%S"
)

fh = logging.FileHandler("logs/db.log", mode="a")
fh.setFormatter(formatter)

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)


class Status(Enum):
    UNPROCESSED = "unprocessed"
    QUEUED = "queued"
    PREPROCESSING = "preprocessing"
    PREPROCESSED = "preprocessed"
    INFERENCING = "inferencing"
    PROCESSED = "processed"
    PREPROCESS_FAILED = "preprocess_failed"
    INFERENCE_FAILED = "inference_failed"


CREATE_TABLE_IMAGES = """
CREATE TABLE IF NOT EXISTS images_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    image_name TEXT NOT NULL,
    source_path TEXT NOT NULL,
    status TEXT DEFAULT '',
    comment TEXT DEFAULT ''
)
"""

INSERT_IMAGE = """
INSERT INTO images_status (image_name, source_path, status) VALUES (?, ?, ?)
"""

INSERT_IMAGE_WITH_COMMENT = """
INSERT INTO images_status (image_name, source_path, status, comment) VALUES (?, ?, ?, ?)
"""

SELECT_NEXT_BY_STATUS = """
SELECT id, image_name, source_path FROM images_status
WHERE status = ? ORDER BY id LIMIT 1
"""

UPDATE_STATUS = """
UPDATE images_status SET status=? WHERE image_name=?
"""

UPDATE_STATUS_BY_PATH = """
UPDATE images_status SET status=? WHERE source_path=?
"""

SELECT_ALL = """
SELECT * FROM images_status
"""

SELECT_STATUS_BY_PATH = "SELECT status FROM images_status WHERE source_path = ?"

UNPROCESSED_FILENAME = "dtm.tif"


class ImageRepository:
    def __init__(self, db_file: Path = DB_FILE):
        self.db_file = db_file

    def _get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_file)

    def create_database(self) -> None:
        self.db_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._get_conn() as conn:
                conn.execute(CREATE_TABLE_IMAGES)
                conn.commit()
            logging.info("Database created or already exists")
        except Exception:
            logging.exception("Failed to create database")

    def insert_image(self, path: Path, status: Status = Status.UNPROCESSED) -> None:
        try:
            with self._get_conn() as conn:
                conn.execute(INSERT_IMAGE, (path.name, str(path), status.value))
                conn.commit()
            logging.info(f"Inserted image into DB: {path.name} [{status.value}]")
        except Exception:
            logging.exception(f"Failed to insert image {path}")

    def insert_images_from_directory(self, image_dir: Path = IMAGE_DIR) -> None:
        try:
            with self._get_conn() as conn:
                for file_path in image_dir.rglob("*"): 
                    if file_path.is_file() and file_path.name.lower() == "dtm.tif" and file_path.parent.name == "2_dtm":
                        conn.execute(
                            INSERT_IMAGE_WITH_COMMENT,
                            (file_path.name, str(file_path), Status.UNPROCESSED.value, ""),
                        )
                conn.commit()
            logging.info("Images inserted from directory into DB")
        except Exception:
            logging.exception("Failed to insert images from directory")


    def get_status(self, path: Path) -> Optional[Status]:
        try:
            with self._get_conn() as conn:
                cur = conn.execute(SELECT_STATUS_BY_PATH, (str(path),))
                row = cur.fetchone()
                if row:
                    return Status(row[0])
                else:
                    return None
        except Exception:
            logging.exception(f"Failed to get status for {path}")
            return None

    def get_next(self, status: Status) -> Optional[Dict[str, Any]]:
        """Get the next image with a given status."""
        try:
            with self._get_conn() as conn:
                cur = conn.execute(SELECT_NEXT_BY_STATUS, (status.value,))
                row = cur.fetchone()
                if not row:
                    return None
                return {"id": row[0], "image_name": row[1], "source_path": Path(row[2])}
        except Exception:
            logging.exception(f"Failed to fetch next image with status {status.value}")
            return None

    def get_next_unprocessed(self) -> Optional[Dict[str, Any]]:
        return self.get_next(Status.UNPROCESSED)

    def update_status(self, path: Path, status: Status) -> None:
        try:
            with self._get_conn() as conn:
                conn.execute(UPDATE_STATUS_BY_PATH, (status.value, str(path)))
                conn.commit()
            logging.info(f"Updated {path} -> {status.value}")
        except Exception:
            logging.exception(f"Failed to update status for {path}")

    def inspect_db(self) -> List[Tuple]:
        try:
            with self._get_conn() as conn:
                cur = conn.execute(SELECT_ALL)
                rows = cur.fetchall()
                for row in rows:
                    logging.info(row)
                return rows
        except Exception:
            logging.exception("Failed to inspect database")
            return []


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQLite database helper for image inference pipeline")
    parser.add_argument("--create", action="store_true", help="Create new database")
    parser.add_argument("--insert-dir", action="store_true", help="Insert all images from IMAGE_DIR")
    parser.add_argument("--inspect", action="store_true", help="Inspect contents of the database")
    args = parser.parse_args()

    repo = ImageRepository()

    if args.create:
        repo.create_database()
        repo.insert_images_from_directory()
    if args.insert_dir:
        repo.insert_images_from_directory()
    if args.inspect:
        repo.inspect_db()
