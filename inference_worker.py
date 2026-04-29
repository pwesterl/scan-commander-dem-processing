import os
import sys
import json
import time
import logging
import subprocess
import pika
from pathlib import Path
import geopandas as gpd
from utils.rabbit_helper import RabbitMQClient
from utils.helper_functions import get_areal_id
from utils.logger import LoggerFactory

logger = LoggerFactory.get_logger("inference_worker", "inference_worker.log")

DEFAULT_MODEL_PATH = Path("trainedModels/InstanceSegmentation")
DEFAULT_INFERENCE_SCRIPT_DIR = Path("/mnt/e/Peder/repo/geoint-dem-detection/model")
DEFAULT_TOOLS_SCRIPT_DIR = Path("/mnt/e/Peder/repo/geoint-dem-detection/tools")
DEFAULT_ROOT_GEOINT_DIR = Path("/mnt/e/Peder/repo/geoint-dem-detection")

MODEL_PATH = Path(os.getenv("MODEL_PATH", str(DEFAULT_MODEL_PATH)))   
INFERENCE_SCRIPT_DIR = Path(os.getenv("INFERENCE_SCRIPT_DIR", str(DEFAULT_INFERENCE_SCRIPT_DIR)))
TOOLS_SCRIPT_DIR = Path(os.getenv("TOOLS_SCRIPT_DIR", str(DEFAULT_TOOLS_SCRIPT_DIR)))
ROOT_GEOINT_DIR = Path(os.getenv("ROOT_GEOINT_DIR", str(DEFAULT_ROOT_GEOINT_DIR)))

sys.path.append(str(TOOLS_SCRIPT_DIR))
from fix_geotiff import fix_directory
from DepthToWaterStreams import process_dtw

MODEL_MAP = {
    "kolbotten": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceDetectron2InstanceSegmentationKolbotten.py" #Kolbottnar
    , 'checkpoint' : MODEL_PATH / "InstanceSegmentation" / "kolbotten20cm.pth"
    , 'resolutions': ['20cm'] } , 
    "fangstgrop": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceDetectron2InstanceSegmentationFangstgropar.py" #Fångstgropar
    , 'checkpoint' : MODEL_PATH / "InstanceSegmentation"/ "fangstgropar10cmImproved.pth" 
    , 'resolutions': ['10cm']},
    "myr": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceMyrplusplus.py" #Myrar 
    , 'checkpoint' : MODEL_PATH / "InstanceSegmentation" / "peder2_200epoch.weights.h5"
    , 'resolutions': ['25cm']},
    "back": {'script_path' : ROOT_GEOINT_DIR / "inferenceVattendragDINOV3.py" # Bäckar
    , 'checkpoint' : MODEL_PATH / "DINOV3" / "backar.pth"
    , 'resolutions': ['10cm']
    ,  'weights_checkpoint' : MODEL_PATH / "DINOV3" / "weights" / "dinov3_vitl16_pretrain_sat493m-eadcf0ff.pth"},
    "korspar": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceKorspar20cmModel.py" # Körspår
    , 'checkpoint' : MODEL_PATH / "UNets" / "Attention_ResUNetkorspar20cm_alpha75.weights.h5"
    , 'resolutions': ['20cm']},
    "vagar": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceMasterModel.py" # Vägar
    , 'checkpoint' : MODEL_PATH / "UNets" / "Vagar20cm_gamma1_Augmentation.weights.h5"
    , 'resolutions': ['20cm']},
    "vagar_korspar": {'script_path' : INFERENCE_SCRIPT_DIR / "inferenceMasterModel.py" # Vägar
    , 'checkpoint' : MODEL_PATH / "UNets" / "VagarKorspar.weights.h5"
    , 'resolutions': ['20cm']},
}

RUN_MODELS = {
    'kolbotten': os.environ.get('RUN_KOLBOTTEN') == '1', # Kolbottnar
    'fangstgrop': os.environ.get('RUN_FANGSTGROP') == '1', # Fångstgropar
    'myr': os.environ.get('RUN_MYR') == '1', # Myrar
    'back': os.environ.get('RUN_BACK') == '1', #Bäckar
    'vagar': os.environ.get('RUN_VAGAR') == '1', # Vägar
    'korspar': os.environ.get('RUN_KORSPAR') == '1', # Körspår
    'vagar_korspar': os.environ.get('RUN_VAGAR_KORSPAR') == '1', # Vägar + Körspår
}



# internal_rabbit = RabbitMQHelper(
#     host=os.getenv("RABBITMQ_HOST_INTERNAL"),
#     name="internal",
#     logger=logger
# )

# This client listens to the grizzly queue
rabbit = RabbitMQClient(logger=logger)

def get_inference_paths(areal_path: Path) -> list[Path]:
    paths = []

    candidates = [
        areal_path / "preprocessed_10cm",
        areal_path / "preprocessed_20cm",
        areal_path / "seven_band_raster",  # 25cm
    ]

    for folder in candidates:
        if not folder.exists():
            continue

        tif_files = list(folder.glob("*.tif"))

        if not tif_files:
            continue

        if len(tif_files) > 1:
            logger.warning(f"Multiple tif files in {folder}, using first: {[f.name for f in tif_files]}")

        paths.append(tif_files[0])

    return paths

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

def run_dtw_for_areal(image_path: Path, inference_shp_path: Path, areal_id: str, threshold=1.0):
    areal_root = image_path.parent.parent
    preprocessed_25cm_dir = areal_root / "preprocessed_25cm" 
    dem_files = list(preprocessed_25cm_dir.glob("*.tif"))
    if not dem_files:
        raise FileNotFoundError(f"No .tif files found in {preprocessed_25cm_dir}")

    dem_path = dem_files[0]

    if not dem_path.exists():
        logger.warning(f"DEM not found for {areal_id}, skipping DTW")
        return

    if not inference_shp_path.exists():
        logger.warning(f"Model output not found for {areal_id}, skipping DTW")
        return

    dtw_output_dir = inference_shp_path.parent.parent / "dtw"
    dtw_output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Running DTW for {areal_id}")

    process_dtw(
        dem_path=dem_path,
        water_path=inference_shp_path,
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
def build_korspar_command(script_path: Path, checkpoint: Path, image_path: Path, raster_output: Path, vector_output: Path):
    return [
        "python3",
        str(script_path),
        str(image_path),
        str(checkpoint),
        str(raster_output),
        str(vector_output),
    ]


def build_vagar_command(script_path: Path, checkpoint: Path, image_path: Path, raster_output: Path, vector_output: Path):
    return [
        "python3",
        str(script_path),
        str(image_path),
        str(checkpoint),
        str(raster_output),
        str(vector_output),
        "--tile_size", "5000",
        "--margin", "50",
        "--num_classes", "2",
    ]


def build_vagar_korspar_command(script_path: Path, checkpoint: Path, image_path: Path, raster_output: Path, vector_output: Path):
    return [
        "python3",
        str(script_path),
        str(image_path),
        str(checkpoint),
        str(raster_output),
        str(vector_output),
        "--tile_size", "5000",
        "--margin", "50",
        "--num_classes", "3",
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
            output_vector_dir
        )

    if model_key in ("kolbotten", "fangstgrop"):
        return build_detectron_command(
            script_path,
            checkpoint,
            image_path,
            output_raster_dir,
            output_vector_dir
        )
    if model_key == "korspar":
        return build_korspar_command(
            script_path,
            checkpoint,
            image_path,
            output_raster_dir,
            output_vector_dir
        )
    if model_key == "vagar":
        return build_vagar_command(
            script_path,
            checkpoint,
            image_path,
            output_raster_dir,
            output_vector_dir
        )
    if model_key == "vagar_korspar":
        return build_vagar_korspar_command(
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


def create_shp_file_from_gpkg(gpkg_path: Path) -> Path:
    if not gpkg_path.exists():
        raise FileNotFoundError(f"GPKG not found: {gpkg_path}")

    shp_path = gpkg_path.with_suffix(".shp")

    gdf = gpd.read_file(gpkg_path)
    gdf.to_file(shp_path, driver="ESRI Shapefile")

    return shp_path


def run_postprocessing_if_needed(model_key: str,
                                  image_path: Path,
                                  output_raster_dir: Path,
                                  areal_id: str):

    if model_key == "back":
        gpkg_files = list(output_raster_dir.glob("*.gpkg"))

        if not gpkg_files:
            raise FileNotFoundError(
                f"No .gpkg file found in {output_raster_dir}"
            )

        if len(gpkg_files) > 1:
            raise RuntimeError(
                f"Multiple .gpkg files found in {output_raster_dir}. "
                f"Be explicit about which one to use: {gpkg_files}"
            )

        gpkg_path = gpkg_files[0]
        print(f"Using GPKG for DTW: {gpkg_path}")
        shp_path = create_shp_file_from_gpkg(gpkg_path)
        run_dtw_for_areal(image_path, shp_path, areal_id)

def run_inference(image_path: Path,
                  model_key: str,
                  model_info: dict,
                  output_root: Path,
                  areal_id: str):

    output_raster_dir = output_root / model_key / "raster"
    output_vector_dir = output_root / model_key / "vector"
    output_raster_dir.mkdir(parents=True, exist_ok=True)
    output_vector_dir.mkdir(parents=True, exist_ok=True)
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
        output_vector_dir,
        areal_id
    )

    # Only fail if no output produced
    if result.returncode != 0 and (
        not output_vector_dir.exists() or not any(output_vector_dir.iterdir())
    ):
        raise RuntimeError(f"{model_key} inference failed: {result.stderr}")

def inference_callback(ch, method, properties, body):
    job = json.loads(body)
    areal_path = Path(job["lidar_output_path"])
    job_id = job.get("job_id")
    task_name = job.get("task_name")
    inference_paths = get_inference_paths(areal_path)

    if job_id and task_name:
        try:
            rabbit.safe_publish("task_results", {
                "job_id": job_id, "task_name": task_name, "status": "STARTED"
            })
        except Exception:
            logger.exception("Failed to send STARTED message")

    task_status = "DONE"
    start_total = time.perf_counter()

    if not inference_paths:
        logger.warning(f"No inference inputs found for {areal_path}")
    for inference_path in inference_paths:
        output_root = areal_path / "inference_output"

        models_to_run = get_models_for_path(inference_path)
        logger.info(f"Models to run for {inference_path}: {[k for k, _ in models_to_run]}")

        if not models_to_run:
            logger.info(f"No models configured for {inference_path}")
            continue

        for model_key, model_info in models_to_run:
            try:
                start_time = time.perf_counter()
                areal_id = get_areal_id(inference_path)
                run_inference(inference_path, model_key, model_info, output_root, areal_id)
                rename_inference_outputs(output_root, model_key, areal_id)
                duration = time.perf_counter() - start_time
                logger.info(f"{model_key} inference completed for {inference_path} in {duration:.2f}s")

            except Exception as e:
                logger.error(f"{model_key} inference failed for {inference_path}: {e}")
                task_status = "FAILED"
                continue

    total_time = time.perf_counter() - start_total
    logger.info(f"All inference attempts finished for {areal_path} in {total_time:.2f}s")
    if job_id and task_name:
        try:
            result = {
                "job_id": job_id,
                "task_name": task_name,
                "status": task_status
            }
            rabbit.safe_publish("task_results", result)
            logger.info(f"Sent result: {result}")
        except Exception as e:
            logger.exception(f"Failed to send result message: {e}")
    rabbit.safe_ack(ch, method.delivery_tag)


def start_consumer():
    while True:
        try:
            conn = rabbit.get_connection()
            channel = conn.channel()
            channel.queue_declare(queue="dem_inference", durable=True)
            channel.queue_declare(queue="task_results", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="dem_inference", on_message_callback=inference_callback)
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
