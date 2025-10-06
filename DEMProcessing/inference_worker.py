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

fh = logging.FileHandler("logs/inference_worker.log", mode="a")
fh.setFormatter(formatter)
logger.addHandler(fh)

# ----------------- Config -----------------
repo = ImageRepository()
MODEL_PATH = Path("trainedModels/InstanceSegmentation/kolbottenFangstgrop.pth")
RASTER_DIR = Path("/mnt/i/Peder/repo/geoint-dem-detection/data/temp/FangstgropKolbotten/Raster")


# ----------------- Helpers -----------------
def get_rabbit_connection(retries=5, delay=5):
    """Blocking RabbitMQ connection with retry logic"""
    for attempt in range(retries):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"RabbitMQ connection failed ({attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to RabbitMQ after multiple retries")


def safe_ack(ch, delivery_tag):
    """Safe acknowledgement: won't crash if connection lost"""
    try:
        ch.basic_ack(delivery_tag=delivery_tag)
    except pika.exceptions.StreamLostError:
        logger.warning("Ack failed, message will be requeued automatically")


def get_areal_id(path: Path):
    match = re.search(r"/(Areal\d+)/", str(path))
    if not match:
        raise ValueError(f"No Areal ID in path: {path}")
    return match.group(1)


def run_inference(image_path: Path, model_path: Path, raster_dir: Path, inference_output_dir: Path):
    logger.info(f"Running inference on {image_path}")
    script = Path("/mnt/i/Peder/repo/geoint-dem-detection/model/inferenceDetectron2InstanceSegmentationFangstgropKolbotten.py")
    env = dict(os.environ)
    repo_root = script.parent.parent
    env["PYTHONPATH"] = str(repo_root) + ":" + env.get("PYTHONPATH", "")

    cmd = [
        "python3",
        str(script),
        str(image_path.parent),
        str(model_path),
        str(raster_dir),
        str(inference_output_dir),
        "--threshold=0.9",
        "--margin=100",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=repo_root, env=env)
    logger.info(f"Inference stdout:\n{result.stdout}")
    logger.error(f"Inference stderr:\n{result.stderr}")

    # Only fail if no output produced
    if result.returncode != 0 and (not inference_output_dir.exists() or not any(inference_output_dir.iterdir())):
        raise RuntimeError(f"Inference failed: {result.stderr}")


# ----------------- Callback -----------------
def inference_callback(ch, method, properties, body):
    job = json.loads(body)
    inference_path = Path(job["inference_path"])
    source_path = Path(job["path"])

    # Avoid duplicate processing
    if repo.get_status(source_path) in [Status.PROCESSED, Status.INFERENCING]:
        logger.info(f"Skipping duplicate inference job {source_path}")
        safe_ack(ch, method.delivery_tag)
        return

    areal_id = get_areal_id(inference_path)
    inference_output_dir = Path("/skog-nas01/scan-data/AW_bearbetning_test") / areal_id / "inference_output"
    repo.update_status(source_path, Status.INFERENCING)

    try:
        run_inference(inference_path, MODEL_PATH, RASTER_DIR, inference_output_dir)
        repo.update_status(source_path, Status.PROCESSED)
        logger.info(f"Inference complete for {source_path}")
    except Exception as e:
        repo.update_status(source_path, Status.INFERENCE_FAILED)
        logger.error(f"Inference failed for {source_path}: {e}")
    finally:
        safe_ack(ch, method.delivery_tag)


# ----------------- Consumer Loop -----------------
def start_consumer():
    while True:
        try:
            conn = get_rabbit_connection()
            channel = conn.channel()
            channel.queue_declare(queue="inference", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="inference", on_message_callback=inference_callback)
            logger.info("Inference worker started and consuming")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            logger.warning("Lost connection to RabbitMQ, retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.exception(f"Unexpected error in consumer loop: {e}")
            time.sleep(5)


# ----------------- Main -----------------
if __name__ == "__main__":
    start_consumer()
