import os
import sys
import json
import time
import logging
import subprocess
import re
import pika
from pathlib import Path
from utils.rabbit_helper import RabbitMQClient
from utils.logger import LoggerFactory
import time
import argparse
from tileExtentToShape import ( #OBS Denna fil är ärvd från geoint-dem-detection för att kunna anpassa inparametrar.
    create_extent_shapefile,
    create_merged_extent_shapefile, # If we ever have the need to use tiles (too large areals perhaps).
)

logger = LoggerFactory.get_logger("preprocess_worker", "preprocess_worker.log")


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

DEFAULT_TOOLS_DIR = Path("/mnt/i/Peder/repo/geoint-dem-detection/tools")
DEFAULT_TEMP_DIR = Path("/mnt/i/Peder/repo/geoint-dem-detection/data/temp")
TOOLS_DIR = Path(os.getenv("TOOLS_DIR", str(DEFAULT_TOOLS_DIR)))
GRIZZLY_MODE = os.environ.get('GRIZZLY_MODE') == '1'
sys.path.append(str(TOOLS_DIR))  # make sure Python can find the modules

from AggregateDEM import process_dem_file
from fix_geotiff import add_sweref99tm_geokey
from resampleDEM import resample_dem
import stackRasters


base_temp_dir = Path(os.getenv("TEMP_DIR", str(DEFAULT_TEMP_DIR)))
worker_id = os.getenv("HOSTNAME") or str(os.getpid())
# Make the container-specific temp dir
TEMP_DIR = base_temp_dir / worker_id
TEMP_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"Using TEMP_DIR: {TEMP_DIR}")

#rabbit = RabbitMQHelper(logger=logger)
# internal_rabbit = RabbitMQHelper(
#     host=os.getenv("RABBITMQ_HOST_INTERNAL"),
#     name="internal",
#     logger=logger
# )

# This client listens to the grizzly queue
rabbit = RabbitMQClient(logger=logger)

def get_areal_name(path: Path):
    match = re.search(r"/(areal\d+|area\d+)/", str(path), re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not find Areal ID in path: {path}")
    return match.group(1)

def rename_output_file(original_path: Path, suffix: str, keep_stem : bool = False) -> Path:
    areal = get_areal_name(original_path)
    if not original_path.exists():
        raise FileNotFoundError(f"File not found: {original_path}")
    if keep_stem:
        new_name = f"{areal}_{suffix}_{original_path.stem}{original_path.suffix}"
    else:  
        new_name = f"{areal}_{suffix}{original_path.suffix}" 
    new_path = original_path.parent / new_name
    original_path.rename(new_path)
    logger.info(f"Renamed {original_path} -> {new_path}")
    return new_path


def aggregate_20cm(image_path: Path) -> Path:
    logger.info(f"Running aggregate_20cm on {image_path}")
    output_dir = image_path.parent.parent / "aggregated_20cm"
    output_dir.mkdir(parents=True, exist_ok=True)
    areal = get_areal_name(image_path)

    output_file = output_dir / f"{areal}_aggregated20cm_{image_path.stem}{image_path.suffix}"

    if os.path.exists(output_file):
        logger.info(f"Aggregated 20cm file already exists, skipping: {output_file}")
        return output_file
    try:
        process_dem_file(str(image_path), str(output_file), 2)
        return output_file
    except Exception as e:
        raise RuntimeError(f"Aggregation failed for {image_path}: {e}")
    
def resample_DEM(image_path: Path) -> Path:
    output_dir = image_path.parent.parent / "resampled_25cm"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / image_path.name
    if output_file.exists():
        logger.info(f"Resampled 25cm file already exists, skipping: {output_file}")
        return output_file
    try:
        logger.info(f"Running resample_dem on {image_path}")
        resample_dem(image_path, output_file, 0.25)
        return output_file
    except Exception as e:
        raise RuntimeError(f"Resample 25cm failed for {image_path}: {e}")

    
def fix_geotif(image_path: Path) -> bool:
    try:
        add_sweref99tm_geokey(str(image_path), 3006)
        return True
    except Exception as e:
        logger.warning(f"fix_geotif failed for {image_path}: {e}")
        return False
    
def get_areal_number(s: str) -> int:
    return int(re.search(r"\d+", s).group())

def areal_to_ortho_filename(areal_name: str) -> str:
    if YEAR=="test":
        base = 24
    else:
        base = int(YEAR[-2:])
    areal_number = get_areal_number(areal_name)
    ortho_number = (base*10000) + int(areal_number)

    if YEAR in ("2024", "test"):
        orto_path = f"Areal{ortho_number}_ortho_clipped.tif"
    else:
        orto_path = f"Areal{ortho_number}_ortho_clipped_{YEAR}.tif"

    return orto_path

    
def areal_to_ortho_path(areal_name: str, orto_dir) -> str:
    filename = areal_to_ortho_filename(areal_name)
    path = os.path.join(orto_dir, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    return path
    
def get_orto_file_path(areal):
    if YEAR == "test":
        orto_root = DATA_ROOT / f"image-process-2024/orto"
    else:
        orto_root = DATA_ROOT / f"image-process-{YEAR}/orto"
    orto_path = areal_to_ortho_path(areal, orto_root)
    return str(orto_path)

    
def stack_rasters(image_path, topograpy_path):
    areal = get_areal_name(image_path)

    chm_path = str(DATA_ROOT / YEAR_PATHS_MAP[YEAR] / areal / "4_chm" / "chm.tif")
    orto_path = get_orto_file_path(areal)

    chm_out = str(DATA_ROOT / YEAR_PATHS_MAP[YEAR] / areal / "chm_tile_output"  / "chmExtent.shp")
    orto_out = str(DATA_ROOT / YEAR_PATHS_MAP[YEAR] / areal / "orto_tile_output" / "ortoExtent.shp")
    topo_out =  str(DATA_ROOT / YEAR_PATHS_MAP[YEAR] / areal / "topography_tile_output" / "topoExtent.shp")
    seven_band_out = str(DATA_ROOT / YEAR_PATHS_MAP[YEAR] / areal / "seven_band_raster")

    tileExtent_tasks = [
        (chm_path, chm_out),
        (orto_path, orto_out),
        (str(topograpy_path), topo_out),  
    ]


    for input_path, output_path in tileExtent_tasks:
        if Path(output_path).exists():
            logger.info(f"Extent already exists, skipping: {output_path}")
            continue
        logger.info(f"Running tile extent for: {input_path}")
        create_extent_shapefile(input_path, output_path)
    
    desired_output = Path(seven_band_out) / f"{areal}_seven_band_25cm.tif"

    if desired_output.exists():
        logger.info(f"Seven-band raster already exists: {desired_output}")
        return desired_output

    logger.info(f"Stacking rasters for areal: {areal}")
    args = argparse.Namespace(
        canopy=str(chm_out),
        topo=str(topo_out),
        ortho=str(orto_out),
        outdir=str(seven_band_out)
    )
    stackRasters.main(args)
    
    seven_band_out = Path(seven_band_out)
    tif_files = list(seven_band_out.glob("*.tif"))

    if not tif_files:
        raise RuntimeError(f"No output GeoTIFF produced in {seven_band_out}")

    if len(tif_files) > 1:
        logger.warning(
            f"Multiple GeoTIFFs found, using first one: {[p.name for p in tif_files]}"
        )

    produced = tif_files[0]
    produced.rename(desired_output)

    logger.info(f"Seven-band raster created: {desired_output}")

    return desired_output


def preprocess_image(image_path: Path, aggregation = "10", combine_rasters = False, max_workers: int = 6) -> Path:
    preprocess_output_dir = image_path.parent.parent / f"preprocessed_{aggregation}cm"
    output_file = Path(preprocess_output_dir) /  image_path.name 
    existing_tifs = list(preprocess_output_dir.glob("*.tif"))

    if existing_tifs:
        logger.info(f"Preprocessed .tif files already exist, skipping: {[f.name for f in existing_tifs]}")
        # Skip processing
        return existing_tifs[0]
    
    logger.info(f"Preprocessing {image_path}")
    script = TOOLS_DIR / "concatenatedTopographyThreeChannelsParallell.py"
    script_dir = script.parent

    cmd = [
        "python3",
        str(script),
        str(TEMP_DIR),
        str(image_path.parent),
        str(preprocess_output_dir),
        f"--max_workers={max_workers}",
    ]

    env = dict(os.environ)
    env["PYTHONPATH"] = str(script_dir) + ":" + env.get("PYTHONPATH", "")

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=script_dir, env=env)
    logger.info(f"Preprocess stdout:\n{result.stdout}")
    logger.error(f"Preprocess stderr:\n{result.stderr}")
    
    if result.returncode != 0 and not output_file.exists():
        raise RuntimeError(f"Preprocessing failed for {image_path}")
    
    if combine_rasters:
        logger.info(f"Combining rasters for: {image_path.name }")
        output_file = rename_output_file(output_file, f"preprocessed_{aggregation}cm")
        seven_band_output = stack_rasters(image_path, output_file) # Combine the aggregated tif with more rasters
        return seven_band_output if seven_band_output.exists() else None

    return output_file if output_file.exists() else None
    
def preprocess_callback(ch, method, properties, body):
    task_status = "FAILED"
    publish_ok = False

    try:
        job = json.loads(body)

        path = Path(job['lidar_output_path']) / "2_dtm" / "dtm.tif"
        job_id = job.get("job_id")
        task_name = job.get("task_name")

        if job_id and task_name:
            rabbit.safe_publish("task_results", {"job_id": job_id, "task_name": task_name, "status": "STARTED"})

        start_total = time.perf_counter()

        has_sweref = fix_geotif(path)
        aggregated_path = aggregate_20cm(path)
        resampled_dem_path = resample_DEM(path)

        preprocessed_files = []

        image_tasks = [
            (path, "10", False),
            (aggregated_path, "20", False),
            (resampled_dem_path, "25", True),
        ]

        for img, agg, combine_rasters in image_tasks:
            logger.info(f"Running preprocessing on {img}")
            preprocessed = preprocess_image(
                img,
                aggregation=agg,
                combine_rasters=combine_rasters
            )

            if preprocessed:
                if not combine_rasters:
                    suffix = f"preprocessed_{agg}cm"
                    preprocessed = rename_output_file(preprocessed, suffix)

                preprocessed_files.append(preprocessed)

        if preprocessed_files:
            task_status = "DONE"
        else:
            task_status = "FAILED"

    except Exception:
        logger.exception(f"Preprocessing failed for {path}")
        task_status = "FAILED"

    finally:
        try:
            if job_id and task_name:
                result = {
                    "job_id": job_id,
                    "task_name": task_name,
                    "status": task_status
                }

                rabbit.safe_publish("task_results", result)
                publish_ok = True

                logger.info(f"Sent result: {result}")

        except Exception:
            logger.exception("Failed to publish result")

        if publish_ok and ch is not None and method is not None:
            rabbit.safe_ack(ch, method.delivery_tag)


def start_consumer():
    while True:
        try:
            connection = rabbit.get_connection()
            channel = connection.channel()
            channel.queue_declare(queue="dem_preprocessing", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="dem_preprocessing", on_message_callback=preprocess_callback)
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
