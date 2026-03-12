import time
import logging
import json
import sys
from pathlib import Path
import pika
import os
from utils.db_utils import PreprocessRepository, Status
from utils.rabbit_helper import RabbitMQHelper
from utils.logger import LoggerFactory

logger = LoggerFactory.get_logger("publisher", "publisher.log")

rabbit = RabbitMQHelper(logger=logger)

repo = PreprocessRepository()
QUEUE_POLL_INTERVAL = 5

DATA_ROOT = Path(os.getenv("DATA_ROOT", "/skog-nas01/scan-data/"))
YEAR = os.getenv("DELIVERY_YEAR", "test")

YEAR_PATHS_MAP = {
    "2021" : "tbd",
    "2022" : "tbd",
    "2023" : "tbd",
    "2024" : "AW_bearbetning",
    "2025" : "AW_bearbetning_2025",
    "test" : "AW_bearbetning_test"
}


def send_areals_to_queue():
    data_path = DATA_ROOT / YEAR_PATHS_MAP[YEAR]
    logger.info(f"🔍 Recursively scanning all subfolders for unprocessed Areal/Area in {data_path} directories...")

    # --- get all areals from DB ---
    processed_areals = repo.get_all_processed_areals()
    processed_areals = {a.lower() for a in processed_areals}

    # --- find all directories named Areal* or at any depth ---
    disk_areals = []
    for root, dirs, files in os.walk(data_path):
        for d in dirs:
            d_lower = d.lower()
            if d_lower.startswith("areal"):
                disk_areals.append(Path(root) / d)

    logger.info(f"🧭 Found {len(disk_areals)} Areal directories on disk.")

    missing = []
    for areal_dir in disk_areals:
        areal_name = areal_dir.name.lower()
        print(areal_dir, areal_name)
        if areal_name not in processed_areals:
            dtm_path = areal_dir / "2_dtm" / "dtm.tif"
            if dtm_path.exists():
                rabbit.safe_publish("preprocess", {"path": str(dtm_path)})
                logger.info(f"📤 Queued unprocessed Areal: {areal_name} ({dtm_path})")
                missing.append(areal_name)
            else:
                logger.warning(f"⚠️ Missing dtm.tif for {areal_name}: {dtm_path}")

    if missing:
        logger.info(f"✅ Queued {len(missing)} unprocessed Areals: {', '.join(missing[:5])}{'...' if len(missing) > 5 else ''}")
    else:
        logger.info("✅ All Areals on disk are already preprocessed or queued.")




def main():
    send_areals_to_queue()
    while True:
        job = repo.get_next_unprocessed()
        if job:
            path = Path(job["source_path"])
            repo.update_status(path, Status.QUEUED)
            rabbit.safe_publish("preprocess", {"path": str(path)})
        else:
            time.sleep(QUEUE_POLL_INTERVAL)

if __name__ == "__main__":
    main()
