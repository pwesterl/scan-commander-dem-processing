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
DEFAULT_INFERENCE_SCRIPT_DIR = Path("/mnt/e/Peder/repo/geoint-dem-detection/model")
DEFAULT_TOOLS_SCRIPT_DIR = Path("/mnt/e/Peder/repo/geoint-dem-detection/tools")
DEFAULT_ROOT_GEOINT_DIR = Path("/mnt/e/Peder/repo/geoint-dem-detection")

# Use environment variables if they exist, otherwise fallback to defaults
MODEL_PATH = Path(os.getenv("MODEL_PATH", str(DEFAULT_MODEL_PATH)))   
INFERENCE_SCRIPT_DIR = Path(os.getenv("INFERENCE_SCRIPT_DIR", str(DEFAULT_INFERENCE_SCRIPT_DIR)))
TOOLS_SCRIPT_DIR = Path(os.getenv("TOOLS_SCRIPT_DIR", str(DEFAULT_TOOLS_SCRIPT_DIR)))
ROOT_GEOINT_DIR = Path(os.getenv("ROOT_GEOINT_DIR", str(DEFAULT_ROOT_GEOINT_DIR)))

sys.path.append(str(TOOLS_SCRIPT_DIR))
from fix_geotiff import fix_directory
from DepthToWaterStreams import process_dtw

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))

MODEL_MAP = {
    "kolbotten": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceDetectron2InstanceSegmentationKolbotten.py"
    , 'checkpoint' : MODEL_PATH / "InstanceSegmentation" / "kolbotten20cm.pth"
    , 'resolutions': ['20cm'] } , 
    "fangstgrop": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceDetectron2InstanceSegmentationFangstgropar.py"
    , 'checkpoint' : MODEL_PATH / "InstanceSegmentation"/ "fangstgropar10cmImproved.pth"
    , 'resolutions': ['10cm']},
    "myr": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceMyrplusplus.py"
    , 'checkpoint' : MODEL_PATH / "InstanceSegmentation" / "peder2_200epoch.weights.h5"
    , 'resolutions': ['25cm']},
    "back": {'script_path' : ROOT_GEOINT_DIR / "inferenceVattendragDINOV3.py" # Bäckar
    , 'checkpoint' : MODEL_PATH / "DINOV3" / "backar.pth"
    , 'resolutions': ['10cm']
    ,  'weights_checkpoint' : ROOT_GEOINT_DIR / "weights" / "dinov3_vitl16_pretrain_sat493m-eadcf0ff.pth"},
}

RUN_MODELS = {
    'kolbotten': os.environ.get('RUN_KOLBOTTEN') == '1',
    'fangstgrop': os.environ.get('RUN_FANGSTGROP') == '1',
    'myr': os.environ.get('RUN_MYR') == '1',
    'back': os.environ.get('RUN_BACK') == '1' #Bäckar
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
    #TODO Skriv om med regex, alt skicka resolution från preprocessor.
    if "10cm" in path_str:
        resolution = "10cm"
    elif "20cm" in path_str:
        resolution = "20cm"
    elif "25cm" in path_str:
        resolution = "25cm"
    else:
        resolution = None

    for model_name, model_info in MODEL_MAP.items():
        if resolution and resolution in model_info.get("resolutions", []):
            if RUN_MODELS[model_name]:
                models.append((model_name, model_info))

    return models

def run_dtw_for_areal(image_path: Path, model_output: Path, areal_id: str, threshold=1.0):
    areal_root = image_path.parent.parent
    preprocessed_25cm_dir = areal_root / "preprocessed_25cm" 
    dem_files = list(preprocessed_25cm_dir.glob("*.tif"))
    if not dem_files:
        raise FileNotFoundError(f"No .tif files found in {preprocessed_25cm_dir}")

    dem_path = dem_files[0]
    model_outputs = list(model_output.glob("*.gpkg"))
    if not model_outputs:
        raise FileNotFoundError(f"No .gpkg files found in {model_outputs}")
    water_path = model_outputs[0]

    if not dem_path.exists():
        logger.warning(f"DEM not found for {areal_id}, skipping DTW")
        return

    if not water_path.exists():
        logger.warning(f"Model output not found for {areal_id}, skipping DTW")
        return

    dtw_output_dir = model_output.parent / "dtw"
    dtw_output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Running DTW for {areal_id}")

    process_dtw(
        dem_path=dem_path,
        water_path=water_path,
        out_dir=dtw_output_dir,
        threshold=threshold
    )

def build_myr_command(script_path: Path, checkpoint: Path, image_path: Path, output_dir: Path):
    return [
        "python3",
        str(script_path),
        "--input", str(image_path),
        "--model_path", str(checkpoint),
        "--output_dir", str(output_dir),
        "--tile_size", "1024",
        "--stride", "512",
        "--n_bands", "7",
        "--threshold", "none"
    ]


def build_back_command(script_path: Path, checkpoint: Path, weights: Path,
                       image_path: Path, output_dir: Path):
    return [
        "python3",
        str(script_path),
        "--input", str(image_path),
        "--weights", str(weights),
        "--checkpoint", str(checkpoint),
        "--output_dir", str(output_dir),
        "--tile_size", "1024",
        "--threshold", "0.5"
    ]


def build_detectron_command(script_path: Path, checkpoint: Path,
                            image_path: Path,
                            raster_dir: Path, vector_dir: Path):
    return [
        "python3",
        str(script_path),
        str(image_path.parent),
        str(checkpoint),
        str(raster_dir),
        str(vector_dir),
        "--threshold=0.9",
        "--margin=100",
    ]

def build_inference_command(model_key: str,
                            model_info: dict,
                            image_path: Path,
                            output_raster_dir: Path,
                            output_vector_dir: Path):

    script_path = model_info["script_path"]
    checkpoint = model_info["checkpoint"]

    if model_key == "myr":
        return build_myr_command(script_path, checkpoint, image_path, output_raster_dir)

    if model_key == "back":
        return build_back_command(
            script_path,
            checkpoint,
            model_info["weights_checkpoint"],
            image_path,
            output_raster_dir
        )

    if model_key in ("kolbotten", "fangstgrop"):
        return build_detectron_command(
            script_path,
            checkpoint,
            image_path,
            output_raster_dir,
            output_vector_dir
        )

    raise ValueError(f"Unknown model_key: {model_key}")

def execute_command(cmd: list, cwd: Path, env: dict, model_key: str):
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=cwd,
        env=env
    )

    logger.info(f"[{model_key}] Inference stdout:\n{result.stdout}")
    logger.error(f"[{model_key}] Inference stderr:\n{result.stderr}")

    return result

def run_postprocessing_if_needed(model_key: str,
                                  image_path: Path,
                                  output_raster_dir: Path,
                                  areal_id: str):

    if model_key == "back":
        run_dtw_for_areal(image_path, output_raster_dir, areal_id)

def run_inference(image_path: Path,
                  model_key: str,
                  model_info: dict,
                  output_root: Path,
                  areal_id: str):

    output_raster_dir = output_root / model_key / "raster"
    output_vector_dir = output_root / model_key / "vector"

    logger.info(f"Running {model_key} inference on {image_path}")

    env = dict(os.environ)
    repo_root = INFERENCE_SCRIPT_DIR.parent.parent
    env["PYTHONPATH"] = str(repo_root) + ":" + env.get("PYTHONPATH", "")

    cmd = build_inference_command(
        model_key,
        model_info,
        image_path,
        output_raster_dir,
        output_vector_dir
    )

    result = execute_command(cmd, repo_root, env, model_key)

    fix_directory(str(output_raster_dir))

    run_postprocessing_if_needed(
        model_key,
        image_path,
        output_raster_dir,
        areal_id
    )

    # Only fail if no output produced
    if result.returncode != 0 and (
        not output_vector_dir.exists() or not any(output_vector_dir.iterdir())
    ):
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
            areal_id = get_areal_id(inference_path)
            run_inference(inference_path, model_key, model_info, output_root, areal_id)
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
