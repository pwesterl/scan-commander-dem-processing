import os
import sys
import json
import time
import logging
import subprocess
import re
import pika
from pathlib import Path
from db_utils import PreprocessRepository, Status
import time

# ----------------- Logging -----------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
Path("logs").mkdir(exist_ok=True)
formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

fh = logging.FileHandler("logs/preprocess_worker.log", mode="a")
fh.setFormatter(formatter)
logger.addHandler(fh)


repo = PreprocessRepository()
DEFAULT_TOOLS_DIR = Path("/mnt/i/Peder/repo/geoint-dem-detection/tools")
DEFAULT_TEMP_DIR = Path("/mnt/i/Peder/repo/geoint-dem-detection/data/temp")
DEFAULT_OUTPUT_ROOT = Path("/skog-nas01/scan-data/AW_bearbetning_test")

TOOLS_DIR = Path(os.getenv("TOOLS_DIR", str(DEFAULT_TOOLS_DIR)))
sys.path.append(str(TOOLS_DIR))  # make sure Python can find the modules
from AggregateDEM import process_dem_file
from fix_geotiff import add_sweref99tm_geokey
base_temp_dir = Path(os.getenv("TEMP_DIR", str(DEFAULT_TEMP_DIR)))
worker_id = os.getenv("HOSTNAME") or str(os.getpid())
# Make the container-specific temp dir
TEMP_DIR = base_temp_dir / worker_id
TEMP_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f"Using TEMP_DIR: {TEMP_DIR}")

OUTPUT_ROOT = Path(os.getenv("OUTPUT_DIR", str(DEFAULT_OUTPUT_ROOT)))


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))

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


def safe_ack(ch, delivery_tag):
    try:
        ch.basic_ack(delivery_tag=delivery_tag)
    except pika.exceptions.StreamLostError:
        logger.warning("Ack failed, message will be requeued automatically")


def get_areal_id(path: Path):
    match = re.search(r"/(areal\d+|area\d+)/", str(path), re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not find Areal ID in path: {path}")
    return match.group(1).lower()

def rename_output_file(original_path: Path, suffix: str, keep_stem : bool = False) -> Path:
    areal = get_areal_id(original_path)
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
    output_file = output_dir / image_path.name
    if output_file.exists():
        logger.info(f"Aggregated 20cm file already exists, skipping: {output_file}")
        return output_file
    try:
        process_dem_file(str(image_path), str(output_file), 2)
        return output_file
    except Exception as e:
        raise RuntimeError(f"Aggregation failed for {image_path}: {e}")
    
def fix_geotif(image_path: Path) -> bool:
    try:
        add_sweref99tm_geokey(str(image_path), 3006)
        return True
    except Exception as e:
        logger.warning(f"fix_geotif failed for {image_path}: {e}")
        return False

def preprocess_image(image_path: Path, aggregation = "10", max_workers: int = 6) -> Path:
    preprocess_output_dir = image_path.parent.parent / f"preprocessed_{aggregation}cm"
    output_file = Path(preprocess_output_dir) /  image_path.name 
    if output_file.exists():
        logger.info(f"Preprocessed file already exists, skipping: {output_file}")
        return None
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

    return output_file if output_file.exists() else None
    
def preprocess_callback(ch, method, properties, body):
    job = json.loads(body)
    path = Path(job["path"])

    if repo.get_status(path) in [Status.PREPROCESSED, Status.PROCESSED, Status.INFERENCING, Status.PREPROCESSING]:
        logger.info(f"Skipping already processed/in-progress job {path}")
        safe_ack(ch, method.delivery_tag)
        return

    repo.update_status(path, Status.PREPROCESSING)
    try:
        start_total = time.perf_counter()
        has_sweref = fix_geotif(path)
        if not has_sweref:
            logger.info("fix_geotif failed, or has already been referenced")
        agg_start = time.perf_counter()
        aggregated_path = aggregate_20cm(path)
        #TODO: Lägg till funktion för resampleDEM till 25 (samma upplösning som höjdraster) returnerna path
        #TODO Lägg till funktioner för 
        aggregated_path = rename_output_file(aggregated_path, "aggregated20cm", keep_stem =True)
        agg_duration = time.perf_counter() - agg_start
        logger.info(f"Aggregate_20cm completed on {aggregated_path} in {agg_duration:.2f}s")
        preprocessed_files = []
        image_tasks = [
            (path, "10"),
            (aggregated_path, "20"), #TODO lägg till path från resampleDEM och kör denna som en image_task
        ]
        for img, agg in image_tasks:
            try:
                logger.info(f"Running preprocessing on {img} (aggregation={agg}cm)")
                step_start = time.perf_counter()
                preprocessed = preprocess_image(img, aggregation=agg) #TODO lägg till flagga för stack_rasters (7bands modeller)
                #implementera kod i preprocess_image för att köra stackraster
                step_duration = time.perf_counter() - step_start

                if preprocessed:
                    suffix = f"preprocessed_{agg}cm"
                    preprocessed = rename_output_file(preprocessed, suffix)
                    preprocessed_files.append(preprocessed)
                    logger.info(f"Preprocessing completed for {img} in {step_duration:.2f}s")
                    safe_publish("inference", {"path": str(path), "inference_path": str(preprocessed)})
                    logger.info(f"Queued {preprocessed} for inference")
                else:
                    logger.warning(f"Preprocessing produced no output for {img} (took {step_duration:.2f}s)")
            except Exception as e:
                logger.error(f"Preprocessing callback failed for {img} after {time.perf_counter() - step_start:.2f}s: {e}")    

        total_duration = time.perf_counter() - start_total

        if preprocessed_files:
            repo.update_status(path, Status.PREPROCESSED)
            logger.info(f"All preprocessing completed for {path} in {total_duration:.2f}s")
        else:
            repo.update_status(path, Status.PREPROCESS_FAILED)
            logger.warning(f"No preprocessed outputs created for {path} (total time {total_duration:.2f}s)")

    except Exception as e:
        repo.update_status(path, Status.PREPROCESS_FAILED)
        logger.error(f"Preprocessing failed for {path}: {e}")
    finally:
        safe_ack(ch, method.delivery_tag)



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
