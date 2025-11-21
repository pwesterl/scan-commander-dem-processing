import os
import sys
import json
import time
import logging
import subprocess
import re
import pika
from pathlib import Path
from db_utils import InferenceRepository, Status

# ----------------- Logging -----------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
Path("logs").mkdir(exist_ok=True)
formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

fh = logging.FileHandler("logs/inference_worker.log", mode="a")
fh.setFormatter(formatter)
logger.addHandler(fh)

repo = InferenceRepository()
DEFAULT_MODEL_PATH = Path("trainedModels/InstanceSegmentation")
DEFAULT_INFERENCE_SCRIPT_DIR = Path("/mnt/i/Peder/repo/geoint-dem-detection/model")

# Use environment variables if they exist, otherwise fallback to defaults
MODEL_PATH = Path(os.getenv("MODEL_PATH", str(DEFAULT_MODEL_PATH)))   
INFERENCE_SCRIPT_DIR = Path(os.getenv("INFERENCE_SCRIPT_DIR", str(DEFAULT_INFERENCE_SCRIPT_DIR)))

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))

MODEL_MAP = {
    "kolbotten": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceDetectron2InstanceSegmentationKolbotten.py"
    , 'checkpoint' : MODEL_PATH / "kolbotten20cm.pth"
    , 'resolutions': ['20cm'] } , 
    "fangstgrop": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceDetectron2InstanceSegmentationFangstgropar.py"
    , 'checkpoint' : MODEL_PATH / "fangstgropar10cm.pth"
    , 'resolutions': ['10cm']},
}

def get_rabbit_connection(retries=5, delay=5):
    for attempt in range(retries):
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
            )
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
    match = re.search(r"/(areal\d+|area\d+)/", str(path), re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not find Areal ID in path: {path}")
    return match.group(1).lower()
    
def rename_inference_outputs(model_output_root: Path, model_key: str, areal_id: str) -> dict:
    renamed = {'raster': None, 'vector': []}
    raster_dir = model_output_root / model_key / "raster"
    if raster_dir.exists():
        for f in raster_dir.glob("*.tif"):
            new_raster_path = raster_dir / f"{areal_id}_inference.tif"
            f.rename(new_raster_path)
            renamed['raster'] = new_raster_path
            logger.info(f"Renamed raster to: {new_raster_path}")

    vector_dir = model_output_root / model_key / "vector"
    if vector_dir.exists():
        stems = set(f.stem for f in vector_dir.iterdir())
        for old_stem in stems:
            for f in vector_dir.iterdir():
                if f.stem == old_stem:
                    new_path = vector_dir / f"{areal_id}_inference{f.suffix}"
                    f.rename(new_path)
                    renamed['vector'].append(new_path)
                    logger.info(f"Renamed vector file {f.name} -> {new_path.name}")

    return renamed


def get_models_for_path(inference_path: Path):
    path_str = str(inference_path).lower()
    logger.info(f"DEBUG: checking inference_path={path_str}")
    models = []
    if "10cm" in path_str:
        resolution = "10cm"
    elif "20cm" in path_str:
        resolution = "20cm"
    else:
        resolution = None

    for model_name, model_info in MODEL_MAP.items():
        if resolution and resolution in model_info.get("resolutions", []):
            models.append((model_name, model_info   ))

    return models

def run_inference(image_path: Path, model_key: str, model_info: dict, output_root: Path):
    script_path = model_info["script_path"]
    checkpoint = model_info["checkpoint"]

    output_raster_dir = output_root / model_key / "raster"
    output_vector_dir = output_root / model_key / "vector"
    #output_raster_dir.mkdir(parents=True, exist_ok=True)
    #output_vector_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Running {model_key} inference on {image_path}")
    env = dict(os.environ)
    repo_root = INFERENCE_SCRIPT_DIR.parent.parent
    env["PYTHONPATH"] = str(repo_root) + ":" + env.get("PYTHONPATH", "")

    cmd = [
        "python3",
        str(script_path),
        str(image_path.parent),
        str(checkpoint),
        str(output_raster_dir),
        str(output_vector_dir),
        "--threshold=0.9",
        "--margin=100",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=repo_root, env=env)
    logger.info(f"[{model_key}] Inference stdout:\n{result.stdout}")
    logger.error(f"[{model_key}] Inference stderr:\n{result.stderr}")

    # Only fail if no output produced
    if result.returncode != 0 and (not output_vector_dir.exists() or not any(output_vector_dir.iterdir())):
        raise RuntimeError(f"{model_key} inference failed: {result.stderr}")


def inference_callback(ch, method, properties, body):
    job = json.loads(body)
    inference_path = Path(job["inference_path"])
    source_path = Path(job["path"])
    output_root = inference_path.parent.parent / "inference_output"

    print(f"Inference job received: {inference_path}")

    start_total = time.perf_counter()
    models_to_run = get_models_for_path(inference_path)
    logger.info(f"Models to run for {inference_path}: {[k for k, _ in models_to_run]}")

    if not models_to_run:
        logger.info(f"No models configured for {inference_path}")
        safe_ack(ch, method.delivery_tag)
        return

    for model_key, model_info in models_to_run:
        current_status = repo.get_status(source_path, model_key)

        if current_status in [Status.INFERENCING, Status.PROCESSED]:
            logger.info(f"Skipping model '{model_key}' — already {current_status.value}")
            continue

        repo.update_status(source_path, inference_path, model_key, Status.INFERENCING, comment="")
        try:
            start_time = time.perf_counter()
            run_inference(inference_path, model_key, model_info, output_root)
            areal_id = get_areal_id(inference_path)
            rename_inference_outputs(output_root, model_key, areal_id)
            duration = time.perf_counter() - start_time

            repo.update_status(
                source_path,
                inference_path,
                model_key,
                Status.PROCESSED,
                comment=""
            )
            logger.info(f"{model_key} inference completed for {inference_path} in {duration:.2f}s")

        except Exception as e:
            repo.update_status(
                source_path,
                inference_path,
                model_key,
                Status.INFERENCE_FAILED,
                comment=str(e)
            )
            logger.error(f"{model_key} inference failed for {inference_path}: {e}")
            # Optional: continue with other models or stop completely
            continue

    total_time = time.perf_counter() - start_total
    logger.info(f"All inference attempts finished for {source_path} in {total_time:.2f}s")
    safe_ack(ch, method.delivery_tag)


def all_models_done_for(source_path: Path) -> bool:
    # om vi vill kolla vilka arealer som är processerade
    rows = repo.list_all()
    relevant = [r for r in rows if r["image_path"] == str(source_path)]
    return all(r["status"] == Status.PROCESSED.value for r in relevant)

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


if __name__ == "__main__":
    start_consumer()
