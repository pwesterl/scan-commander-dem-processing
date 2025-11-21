import os
import sys
import time
import json
import pika
import logging
from pathlib import Path
import re

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

# ---------------- RabbitMQ Config ----------------
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))

def get_areal_id(path: Path):
    match = re.search(r"/(areal\d+|area\d+)/", str(path), re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not find Areal ID in path: {path}")
    return match.group(1).lower()

def get_rabbit_connection(retries=5, delay=5):
    """Try multiple times to connect to RabbitMQ."""
    for attempt in range(retries):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"RabbitMQ connection failed ({attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("❌ Could not connect to RabbitMQ after multiple retries.")

def safe_publish(queue_name, message):
    """Publish a persistent message safely to RabbitMQ."""
    while True:
        try:
            connection = get_rabbit_connection()
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2),  # make message persistent
            )
            connection.close()
            logger.info(f"📤 Published to {queue_name}: {message}")
            break
        except pika.exceptions.AMQPConnectionError:
            logger.warning("Connection lost, retrying publish in 5s...")
            time.sleep(5)

# ---------------- Core Logic ----------------

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
        safe_publish("inference", {"path": path, "inference_path": str(preprocessed20)})

    logger.info(f"✅ Finished sending {len(lines)} inference jobs to queue")


# ---------------- Entrypoint ----------------
if __name__ == "__main__":
    preprocess_txt = os.getenv("PREPROCESS_TXT", "missing_preprocessed_dtm.txt")
    inference_txt = os.getenv("INFERENCE_TXT", "existing_preprocessed_dtm.txt")

    logger.info("🚀 Starting requeue process...")
    requeue_preprocess_jobs(preprocess_txt)
    requeue_inference_jobs(inference_txt)
    logger.info("🏁 Done requeuing missing jobs.")
