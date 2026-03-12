import os
import sys
import time
import json
import pika
import logging
from pathlib import Path
from utils.helper_functions import get_areal_id

# ---------------- Logging ----------------
logger = logging.getLogger("requeue_missing_jobs")
logger.setLevel(logging.INFO)
Path("logs").mkdir(exist_ok=True)
formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler("logs/requeue_missing_jobs.log", mode="a")
fh.setFormatter(formatter)
logger.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)


def requeue_preprocess_jobs(preprocess_txt):
    """Send each path from preprocess TXT file to the 'preprocess' queue."""
    path_file = Path(preprocess_txt)
    if not path_file.exists():
        logger.warning(f"⚠️ Missing file: {preprocess_txt}")
        return

    with path_file.open("r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    logger.info(f"📦 Found {len(lines)} paths in {preprocess_txt}")

    for path in lines:
        safe_publish("preprocess", {"path": path})

    logger.info(f"✅ Finished sending {len(lines)} preprocess jobs to queue")


def requeue_inference_jobs(inference_txt):
    """Send each path from inference TXT file to the 'inference' queue."""
    path_file = Path(inference_txt)
    if not path_file.exists():
        logger.warning(f"⚠️ Missing file: {inference_txt}")
        return

    with path_file.open("r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    logger.info(f"📦 Found {len(lines)} paths in {inference_txt}")

    for path in lines:
        # Construct the expected preprocessed directory path
        areal_id = get_areal_id(Path(path))
        file_10cm = areal_id + "_preprocessed_10cm.tif"
        file_20cm = areal_id + "_preprocessed_20cm.tif"
        preprocessed10 = Path(path).parent.parent / "preprocessed_10cm" / file_10cm
        preprocessed20 = Path(path).parent.parent / "preprocessed_20cm" / file_20cm
        safe_publish("inference", {"path": path, "inference_path": str(preprocessed10)})
        #safe_publish("inference", {"path": path, "inference_path": str(preprocessed20)})

    logger.info(f"✅ Finished sending {len(lines)} inference jobs to queue")


# ---------------- Entrypoint ----------------
if __name__ == "__main__":
    preprocess_txt = os.getenv("PREPROCESS_TXT", "missing_preprocessed_dtm.txt")
    inference_txt = os.getenv("INFERENCE_TXT", "existing_preprocessed_dtm.txt") # Kör existing för att köra om samtliga fangstgropar i detta fall

    logger.info("🚀 Starting requeue process...")
    requeue_preprocess_jobs(preprocess_txt)
    #requeue_inference_jobs(inference_txt)
    logger.info("🏁 Done requeuing missing jobs.")
