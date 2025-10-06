import os
import sys
import json
import time
import logging
import subprocess
import re
import pika
from pathlib import Path
from db_utils import ImageRepository, Status

# ----------------- Logging -----------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

fh = logging.FileHandler("logs/preprocess_worker.log", mode="a")
fh.setFormatter(formatter)
logger.addHandler(fh)

# ----------------- Config -----------------
repo = ImageRepository()
TEMP_DIR = Path("/mnt/i/Peder/repo/geoint-dem-detection/data/temp")


# ----------------- RabbitMQ helpers -----------------
def get_rabbit_connection(retries=5, delay=5):
    """Blocking RabbitMQ connection with retry logic"""
    for attempt in range(retries):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"RabbitMQ connection failed ({attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to RabbitMQ after multiple retries")


def safe_publish(queue_name, message):
    """Publish message with auto-reconnect"""
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


def safe_ack(ch, delivery_tag):
    """Safe acknowledgement for RabbitMQ"""
    try:
        ch.basic_ack(delivery_tag=delivery_tag)
    except pika.exceptions.StreamLostError:
        logger.warning("Ack failed, message will be requeued automatically")


# ----------------- Preprocessing -----------------
def get_areal_id(path: Path):
    match = re.search(r"/(Areal\d+)/", str(path))
    if not match:
        raise ValueError(f"Could not find Areal ID in path: {path}")
    return match.group(1)


def preprocess_image(image_path: Path, temp_dir: Path, max_workers: int = 4) -> Path:
    logger.info(f"Preprocessing {image_path}")
    areal_id = get_areal_id(image_path)
    preprocess_output_dir = Path("/skog-nas01/scan-data/AW_bearbetning_test") / areal_id / "preprocessed"
    script = Path("/mnt/i/Peder/repo/geoint-dem-detection/tools/concatenatedTopographyThreeChannels.py")
    script_dir = script.parent

    cmd = [
        "python3",
        str(script),
        str(temp_dir),
        str(image_path.parent),
        str(preprocess_output_dir),
        f"--max_workers={max_workers}",
    ]

    env = dict(os.environ)
    env["PYTHONPATH"] = str(script_dir) + ":" + env.get("PYTHONPATH", "")

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=script_dir, env=env)
    logger.info(f"Preprocess stdout:\n{result.stdout}")
    logger.error(f"Preprocess stderr:\n{result.stderr}")

    output_file = Path(preprocess_output_dir) / image_path.name
    if result.returncode != 0 and not output_file.exists():
        raise RuntimeError(f"Preprocessing failed for {image_path}")

    return output_file


# ----------------- RabbitMQ callback -----------------
def preprocess_callback(ch, method, properties, body):
    job = json.loads(body)
    path = Path(job["path"])

    if repo.get_status(path) in [Status.PREPROCESSED, Status.PROCESSED, Status.INFERENCING, Status.PREPROCESSING]:
        logger.info(f"Skipping already processed/in-progress job {path}")
        safe_ack(ch, method.delivery_tag)
        return

    repo.update_status(path, Status.PREPROCESSING)
    try:
        inference_path = preprocess_image(path, TEMP_DIR)
        repo.update_status(path, Status.PREPROCESSED)
        safe_publish("inference", {"path": str(path), "inference_path": str(inference_path)})
        logger.info(f"Preprocessed and queued {inference_path} for inference")
    except Exception as e:
        repo.update_status(path, Status.PREPROCESS_FAILED)
        logger.error(f"Preprocessing failed for {path}: {e}")
    finally:
        safe_ack(ch, method.delivery_tag)


# ----------------- Consumer loop -----------------
def start_consumer():
    while True:
        try:
            connection = get_rabbit_connection()
            channel = connection.channel()
            channel.queue_declare(queue="preprocess", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="preprocess", on_message_callback=preprocess_callback)
            logger.info("Preprocess worker started and consuming")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            logger.warning("Lost RabbitMQ connection, retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.exception(f"Unexpected error in consumer loop: {e}")
            time.sleep(5)


# ----------------- Main -----------------
if __name__ == "__main__":
    start_consumer()
