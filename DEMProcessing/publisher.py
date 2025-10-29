import time
import logging
import json
import sys
from pathlib import Path
import pika
import os
from db_utils import PreprocessRepository, Status

# --- Logging ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
Path("logs").mkdir(exist_ok=True)
formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler("logs/publisher.log", mode="a")
fh.setFormatter(formatter)
logger.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

repo = PreprocessRepository()
QUEUE_POLL_INTERVAL = 5

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))

def get_rabbit_connection(retries=5, delay=5):
    retries = 5
    delay = 5
    for attempt in range(retries):
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
            )
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"RabbitMQ connection failed ({attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to RabbitMQ after multiple retries")

def safe_publish(queue_name, message):
    while True:
        try:
            connection = get_rabbit_connection()
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            connection.close()
            logger.info(f"Published job to {queue_name}: {message}")
            break
        except pika.exceptions.AMQPConnectionError:
            logger.warning(f"Connection lost, retrying publish in 5s...")
            time.sleep(5)


def send_unprocessed_areals_to_queue():
    """
    Recursively scan /app/data/AW_bearbetning (all subfolders like 2021, 2022, 2023)
    for Areal/Area directories that are missing in preprocess_status or not yet preprocessed,
    and queue them to RabbitMQ.
    """
    from pathlib import Path

    DATA_ROOT = Path("/app/data/AW_bearbetning")
    logger.info("🔍 Recursively scanning all subfolders for unprocessed Areal/Area directories...")

    # --- get all areals from DB ---
    processed_areals = repo.get_all_processed_areals()
    processed_areals = {a.lower() for a in processed_areals}

    # --- find all directories named Areal* or at any depth ---
    disk_areals = []
    for root, dirs, files in os.walk(DATA_ROOT):
        for d in dirs:
            d_lower = d.lower()
            if d_lower.startswith("areal"):
                disk_areals.append(Path(root) / d)

    logger.info(f"🧭 Found {len(disk_areals)} Areal directories on disk.")

    missing = []
    for areal_dir in disk_areals:
        areal_name = areal_dir.name.lower()
        if areal_name not in processed_areals:
            dtm_path = areal_dir / "2_dtm" / "dtm.tif"
            if dtm_path.exists():
                safe_publish("preprocess", {"path": str(dtm_path)})
                logger.info(f"📤 Queued unprocessed Areal: {areal_name} ({dtm_path})")
                missing.append(areal_name)
            else:
                logger.warning(f"⚠️ Missing dtm.tif for {areal_name}: {dtm_path}")

    if missing:
        logger.info(f"✅ Queued {len(missing)} unprocessed Areals: {', '.join(missing[:5])}{'...' if len(missing) > 5 else ''}")
    else:
        logger.info("✅ All Areals on disk are already preprocessed or queued.")




def main():
    send_unprocessed_areals_to_queue()
    while True:
        job = repo.get_next_unprocessed()
        if job:
            path = Path(job["source_path"])
            repo.update_status(path, Status.QUEUED)
            safe_publish("preprocess", {"path": str(path)})
        else:
            time.sleep(QUEUE_POLL_INTERVAL)

if __name__ == "__main__":
    main()
