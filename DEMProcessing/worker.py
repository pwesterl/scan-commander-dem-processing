import os
import time
import logging
import signal
import sys
import json
from pathlib import Path
import subprocess
import re
import threading
import pika
from db_utils import ImageRepository, Status


logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] %(message)s",
    "%Y-%m-%d %H:%M:%S"
)

fh = logging.FileHandler("logs/worker.log", mode="a")
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

repo = ImageRepository()
QUEUE_POLL_INTERVAL = 5
TEMP_DIR = Path("/mnt/i/Peder/repo/geoint-dem-detection/data/temp")
MODEL_PATH = Path("trainedModels/InstanceSegmentation/kolbottenFangstgrop.pth")
RASTER_DIR = Path("/mnt/i/Peder/repo/geoint-dem-detection/data/temp/FangstgropKolbotten/Raster")


def get_rabbit_connection(retries=5, delay=5):
    """Blocking connection with retry logic"""
    for attempt in range(retries):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"RabbitMQ connection failed ({attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to RabbitMQ after multiple retries")

def safe_publish(queue_name, message):
    """Publish with automatic reconnect if RabbitMQ is down"""
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
            logger.warning(f"Connection lost, retrying publish to {queue_name} in 5s...")
            time.sleep(5)

def safe_ack(ch, delivery_tag):
    """Safe acknowledgement: won’t crash if connection lost"""
    try:
        ch.basic_ack(delivery_tag=delivery_tag)
    except pika.exceptions.StreamLostError:
        logger.warning("Connection lost before ack; message will be requeued automatically by RabbitMQ")

def get_areal_id(path : Path):
    match = re.search(r"/(Areal\d+)/", str(path))
    if not match:
        raise ValueError(f"Could not find Areal ID in path: {path}")
    return match.group(1)

def preprocess_image(image_path: Path, temp_dir: Path, max_workers: int = 4) -> Path:
    logger.info(f"Preprocessing {image_path}")
    areal_id = get_areal_id(image_path)
    preprocess_output_dir = Path("/skog-nas01/scan-data/AW_bearbetning_test") / areal_id / "preprocessed"
    script = Path("/mnt/i/Peder/repo/geoint-dem-detection/tools/concatenatedTopographyThreeChannels.py") # Fix for docker
    script_dir = script.parent

    cmd = [
        "python3",
        str(script),
        str(temp_dir),
        str(image_path.parent),
        str(preprocess_output_dir),
        f"--max_workers={max_workers}",
    ]

    # Ensure WriteGeotiff.py is importable by adding script directory to PYTHONPATH
    env = dict(os.environ)
    env["PYTHONPATH"] = str(script_dir) + ":" + env.get("PYTHONPATH", "")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=script_dir, env=env)
        output_file = Path(preprocess_output_dir) / image_path.name
        if result.returncode != 0:
            logger.warning(f"Preprocess script exited with {result.returncode}. stderr:\n{result.stderr}")
            if output_file.exists():
                logger.info(f"Output {output_file} exists despite non-zero exit code, treating as success")
            else:
                raise RuntimeError(f"Preprocessing failed for {image_path}")
    except subprocess.CalledProcessError as e:
        logger.error(e)
        raise RuntimeError(f"Preprocessing failed for {image_path}") from e

    inference_path = Path(preprocess_output_dir) / image_path.name
    return inference_path


def run_inference(
        image_path: Path,
        model_path: Path,
        raster_dir: Path,
        inference_output_dir: Path,
        threshold: float = 0.9,
        margin: int = 100
    ):
    logger.info(f"Running inference on {image_path}")
    script = Path("/mnt/i/Peder/repo/geoint-dem-detection/model/inferenceDetectron2InstanceSegmentationFangstgropKolbotten.py")
    # Set repo root as PYTHONPATH so relative imports inside the script work
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
        f"--threshold={threshold}",
        f"--margin={margin}",
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=repo_root,
            env=env
        )
        if result.returncode != 0:
            logger.warning(f"Inference script exited with {result.returncode}. stderr:\n{result.stderr}")
            if inference_output_dir.exists() and any(inference_output_dir.iterdir()):
                logger.info(f"Output {inference_output_dir} exists despite non-zero exit code, treating as success")
            else:
                raise RuntimeError(f"Inference script failed for {image_path}")
    except subprocess.CalledProcessError as e:
        logger.error(e)
        raise RuntimeError(f"Inference failed for {image_path}") from e


def preprocess_callback(ch, method, properties, body):
    job = json.loads(body)
    path = Path(job["path"])
    if repo.get_status(path) in [Status.PREPROCESSED, Status.PROCESSED, Status.INFERENCING, Status.PREPROCESSING]:
        logger.info(f"Skipping already processed or in-progress job {path}")
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
        safe_ack(ch, method.delivery_tag) # TODO add fail-safe

def inference_callback(ch, method, properties, body):
    job = json.loads(body)
    inference_path = Path(job["inference_path"])
    source_path = Path(job["path"])
    #Undvik dubletter
    if repo.get_status(source_path) in [Status.PROCESSED, Status.INFERENCING]:
        logger.info(f"Skipping already processed or in-progress inference job {source_path}")
        safe_ack(ch, method.delivery_tag)
        return
    areal_id = get_areal_id(inference_path)
    inference_output_dir = Path("/skog-nas01/scan-data/AW_bearbetning_test") / areal_id / "inference_output"
    repo.update_status(source_path, Status.INFERENCING)
    try:
        run_inference(inference_path, MODEL_PATH, RASTER_DIR, inference_output_dir)
        repo.update_status(source_path, Status.PROCESSED)
        logger.info(f"Inference completed for {source_path}")
    except Exception as e:
        repo.update_status(source_path, Status.INFERENCE_FAILED)
        logger.error(f"Inference failed for {source_path}: {e}")
    finally:
        safe_ack(ch, method.delivery_tag)


def publish_unprocessed_jobs():
    while True:
        job = repo.get_next_unprocessed()
        if job:
            path = Path(job["source_path"])
            repo.update_status(path, Status.QUEUED)
            safe_publish("preprocess", {"path": str(path)})
            logger.info(f"Published job {path} to preprocess queue")
        else:
            time.sleep(QUEUE_POLL_INTERVAL)

def main():
    threading.Thread(target=publish_unprocessed_jobs, daemon=True).start()
    # Preprocess / inference consumer
    def start_consumer(queue_name, callback):
        while True:
            try:
                connection = get_rabbit_connection()
                channel = connection.channel()
                channel.queue_declare(queue=queue_name, durable=True)
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue=queue_name, on_message_callback=callback)
                logger.info(f"Started consumer for {queue_name}")
                channel.start_consuming()
            except pika.exceptions.AMQPConnectionError:
                logger.warning(f"Lost connection to RabbitMQ for {queue_name}, retrying in 5s...")
                time.sleep(5)

    # Start consumers in separate threads
    threading.Thread(target=start_consumer, args=("preprocess", preprocess_callback), daemon=True).start()
    threading.Thread(target=start_consumer, args=("inference", inference_callback), daemon=True).start()

    # Handle graceful shutdown
    def shutdown(sig, frame):
        logger.info("Shutting down worker...")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Keep main thread alive
    while True:
        time.sleep(10)

if __name__ == "__main__":
    main()
