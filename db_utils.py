import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import os
import logging
from pathlib import Path
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
import argparse
import time
# ----------------- Config -----------------
DEFAULT_IMAGE_DIR = Path("/skog-nas01/scan-data/AW_bearbetning_test/")
IMAGE_EXTENSIONS = (".tif",)

# Database connection settings
DB_PARAMS = {
    "dbname": os.getenv("POSTGRES_DB", "geoint"),
    "user": os.getenv("POSTGRES_USER", "geouser"),
    "password": os.getenv("POSTGRES_PASSWORD", "geopass"),
    "host": os.getenv("POSTGRES_HOST", "db"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

# ----------------- Logging -----------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

Path("logs").mkdir(exist_ok=True)
fh = logging.FileHandler("logs/db.log", mode="a")
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

# ----------------- Status Enum -----------------
class Status(Enum):
    UNPROCESSED = "unprocessed"
    QUEUED = "queued"
    PREPROCESSING = "preprocessing"
    PREPROCESSED = "preprocessed"
    INFERENCING = "inferencing"
    PROCESSED = "processed"
    PREPROCESS_FAILED = "preprocess_failed"
    INFERENCE_FAILED = "inference_failed"

# ----------------- SQL Statements -----------------
CREATE_TABLE_PREPROCESS = """
CREATE TABLE IF NOT EXISTS preprocess_status (
    id SERIAL PRIMARY KEY,
    image_name TEXT NOT NULL,
    source_path TEXT NOT NULL,
    status TEXT DEFAULT '',
    comment TEXT DEFAULT '',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_TABLE_INFERENCE = """
CREATE TABLE IF NOT EXISTS inference_status (
    id SERIAL PRIMARY KEY,
    source_path TEXT NOT NULL,
    input_path TEXT NOT NULL, 
    model_name TEXT NOT NULL,
    status TEXT DEFAULT '',
    comment TEXT DEFAULT '',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_path, input_path, model_name)
);
"""

INSERT_IMAGE_IF_NOT_EXISTS = """
INSERT INTO preprocess_status (image_name, source_path, status)
VALUES (%s, %s, %s)
ON CONFLICT (source_path) DO NOTHING
"""
INSERT_IMAGE_WITH_COMMENT = "INSERT INTO preprocess_status (image_name, source_path, status, comment) VALUES (%s, %s, %s, %s)"
SELECT_NEXT_BY_STATUS = "SELECT id, image_name, source_path FROM preprocess_status WHERE status = %s ORDER BY id LIMIT 1"
UPDATE_STATUS_BY_PATH = """
    UPDATE preprocess_status
    SET status = %s,
        last_updated = CURRENT_TIMESTAMP
    WHERE source_path = %s
"""
PREPROCESS_SELECT_ALL = "SELECT * FROM preprocess_status"
INFERENCE_SELECT_ALL = "SELECT * FROM inference_status"
SELECT_STATUS_BY_PATH = "SELECT status FROM preprocess_status WHERE source_path = %s"

UNPROCESSED_FILENAME = "dtm.tif"
UNPROCESSED_DIRNAME = "2_dtm"

# ----------------- Repository Class -----------------
class PreprocessRepository:
    def __init__(self, image_dir : Path = None):
        self.conn_params = DB_PARAMS
        self.image_dir = image_dir

    def _get_conn(self):
        delay = 3
        retries = 10
        for i in range(retries):
            try:
                return psycopg2.connect(**self.conn_params, cursor_factory=RealDictCursor)
            except psycopg2.OperationalError:
                logger.warning(f"Postgres not ready, retry {i+1}/{retries}. Waiting {delay}s...")
                time.sleep(delay)
        raise RuntimeError("Postgres never became ready")

    def create_database(self) -> None:
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(CREATE_TABLE_PREPROCESS)
                    conn.commit()
            logger.info("Database created or already exists in PostgreSQL")
        except Exception:
            logger.exception("Failed to create PostgreSQL database")

    def insert_image(self, path: Path, status: Status = Status.UNPROCESSED) -> None:
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(INSERT_IMAGE_IF_NOT_EXISTS, (path.name, str(path), status.value))
                    conn.commit()
            logger.info(f"Inserted image: {path.name} [{status.value}]")
        except Exception:
            logger.exception(f"Failed to insert image {path}")

    def insert_images_from_directory(self) -> None:
        if not self.image_dir or not self.image_dir.exists():
            raise ValueError(f"Invalid or missing image directory: {self.image_dir}")
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    for file_path in self.image_dir.rglob("*"):
                        if (
                            file_path.is_file()
                            and file_path.parent.name == UNPROCESSED_DIRNAME
                            and file_path.name.lower() == UNPROCESSED_FILENAME
                        ):
                            cur.execute(
                                INSERT_IMAGE_WITH_COMMENT,
                                (file_path.name, str(file_path), Status.UNPROCESSED.value, ""),
                            )
                            logger.info(f"Inserted from directory: {file_path.name.lower() }")
                    conn.commit()
            logger.info(f"Images inserted from directory: {self.image_dir}")
        except Exception:
            logger.exception(f"Failed to insert images from {self.image_dir}")

    def get_all_processed_areals(self):
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT substring(source_path from '.*/(Areal[0-9]+)/.*') AS areal_name
                    FROM preprocess_status
                    WHERE status IN ('preprocessed');
                """)
                rows = cur.fetchall()
                return [r["areal_name"] for r in rows if r["areal_name"]]



    def get_status(self, path: Path) -> Optional[Status]:
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(SELECT_STATUS_BY_PATH, (str(path),))
                    row = cur.fetchone()
                    return Status(row["status"]) if row else None
        except Exception:
            logger.exception(f"Failed to get status for {path}")
            return None

    def get_next(self, status: Status) -> Optional[Dict[str, Any]]:
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(SELECT_NEXT_BY_STATUS, (status.value,))
                    row = cur.fetchone()
                    if row:
                        return {"id": row["id"], "image_name": row["image_name"], "source_path": Path(row["source_path"])}
        except Exception:
            logger.exception(f"Failed to fetch next image with status {status.value}")
        return None

    def get_next_unprocessed(self) -> Optional[Dict[str, Any]]:
        return self.get_next(Status.UNPROCESSED)

    def update_status(self, path: Path, status: Status) -> None:
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(UPDATE_STATUS_BY_PATH, (status.value, str(path)))
                    conn.commit()
            logger.info(f"Updated {path} -> {status.value}")
        except Exception:
            logger.exception(f"Failed to update status for {path}")

    def inspect_db(self) -> List[Dict[str, Any]]:
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(PREPROCESS_SELECT_ALL)
                    rows = cur.fetchall()
                    for row in rows:
                        logger.info(row)
                    return rows
        except Exception:
            logger.exception("Failed to inspect database")
            return []

class InferenceRepository:
    def __init__(self):
        self.conn_params = DB_PARAMS

    def _get_conn(self):
        delay = 3
        retries = 10
        for i in range(retries):
            try:
                return psycopg2.connect(**self.conn_params, cursor_factory=RealDictCursor)
            except psycopg2.OperationalError:
                logger.warning(f"Postgres not ready, retry {i+1}/{retries}. Waiting {delay}s...")
                time.sleep(delay)
        raise RuntimeError("Postgres never became ready")

    def create_database(self):
        """Create inference_status table if it doesn’t exist."""
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute(CREATE_TABLE_INFERENCE)
            conn.commit()
        logger.info("Inference status table created or already exists")

    def update_status(self, image_path: Path, input_path: Path, model_name: str, status: Status, comment: str = ""):
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO inference_status (source_path, input_path, model_name, status, comment)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_path, input_path, model_name)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    comment = EXCLUDED.comment,
                    last_updated = CURRENT_TIMESTAMP;
            """, (str(image_path), str(input_path), model_name, status.value, comment))
            conn.commit()
        logger.info(f"Updated inference status: {image_path} [{model_name}] on {input_path} -> {status.value}")

    def get_status(self, image_path: Path, model_name: str) -> Optional[Status]:
        """Get the current status for a given model/image pair."""
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT status FROM inference_status
                WHERE source_path = %s AND model_name = %s
            """, (str(image_path), model_name))
            row = cur.fetchone()
            return Status(row["status"]) if row else None

    def list_all(self) -> List[Dict[str, Any]]:
        """List all inference jobs and statuses."""
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM inference_status ORDER BY last_updated DESC")
            rows = cur.fetchall()
            for r in rows:
                logger.info(r)
            return rows

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PostgreSQL DB helper for image pipeline")
    parser.add_argument("--create", action="store_true", help="Create new database table")
    parser.add_argument("--insert-dir", action="store_true", help="Insert all images from IMAGE_DIR")
    parser.add_argument("--inspect", action="store_true", help="Inspect database contents")
    args = parser.parse_args()

    repo = PreprocessRepository()
    inference_repo = InferenceRepository()
    if args.create:
        repo.create_database()
        inference_repo.create_database()
    if args.insert_dir:
        repo.insert_images_from_directory()
    if args.inspect:
        repo.inspect_db()