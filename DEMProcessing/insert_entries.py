import psycopg2
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# --- Database connection setup ---
DB_CONFIG = {
    "host": "db",
    "port": 5432,
    "dbname": "geoint",
    "user": "geouser",
    "password": "geopass",
}

def read_paths_from_file(file_path: Path):
    if not file_path.exists():
        logger.error(f"❌ File not found: {file_path}")
        return []
    
    with file_path.open("r", encoding="utf-8") as f:
        paths = [line.strip() for line in f if line.strip()]
    
    logger.info(f"📄 Loaded {len(paths)} paths from {file_path}")
    return paths

def insert_preprocessed_paths(paths):
    """Insert each DTM path into preprocess_status with status='preprocessed'
       and insert into inference_status only if input folders exist, setting status='processed'.
    """
    if not paths:
        logger.warning("⚠️ No paths to insert.")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # --- preprocess_status ---
    insert_preprocess_sql = """
    INSERT INTO preprocess_status (image_name, source_path, status)
    VALUES (%s, %s, %s)
    ON CONFLICT (source_path) DO NOTHING;
    """

    # --- inference_status ---
    insert_inference_sql = """
    INSERT INTO inference_status (source_path, input_path, model_name, status)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (source_path, input_path, model_name) DO NOTHING;
    """

    for path_str in paths:
        path = Path(path_str)
        areal_dir = path.parent.parent  # parent of "2_dtm"
        image_name = areal_dir.name

        # --- preprocess_status ---
        cur.execute(insert_preprocess_sql, (image_name, str(path), "preprocessed"))
        logger.info(f"✅ Inserted/Updated preprocess_status: {image_name} ({path})")

        # --- inference_status ---
        # fangstgrop → preprocessed_10cm
        fangstgrop_input = areal_dir / "preprocessed_10cm"
        if fangstgrop_input.exists() and fangstgrop_input.is_dir():
            cur.execute(insert_inference_sql, (str(path), str(fangstgrop_input), "fangstgrop", "processed"))
            logger.info(f"➡ Queued inference_status: fangstgrop ({fangstgrop_input})")
        else:
            logger.warning(f"⚠️ Missing folder, skipping fangstgrop: {fangstgrop_input}")

        # kolbotten → preprocessed_20cm
        kolbotten_input = areal_dir / "preprocessed_20cm"
        if kolbotten_input.exists() and kolbotten_input.is_dir():
            cur.execute(insert_inference_sql, (str(path), str(kolbotten_input), "kolbotten", "processed"))
            logger.info(f"➡ Queued inference_status: kolbotten ({kolbotten_input})")
        else:
            logger.warning(f"⚠️ Missing folder, skipping kolbotten: {kolbotten_input}")

    conn.commit()
    cur.close()
    conn.close()
    logger.info("💾 All entries inserted/updated successfully.")

if __name__ == "__main__":
    input_file = Path("2024dtm.txt")
    dtm_paths = read_paths_from_file(input_file)
    insert_preprocessed_paths(dtm_paths)
