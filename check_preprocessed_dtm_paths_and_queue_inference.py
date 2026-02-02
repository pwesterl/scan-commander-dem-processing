import psycopg2
import pika
from pathlib import Path
import logging
import json
import os
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "db",
    "port": 5432,
    "dbname": "geoint",
    "user": "geouser",
    "password": "geopass",
}

RABBITMQ_HOST = "rabbitmq"
QUEUE_NAME = "inference"
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
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

def update_db_status(source_path: str, image_name: str = "dtm.tif", status: str = "unprocessed", comment: str = "row changed from script"):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    sql = """
        INSERT INTO preprocess_status (image_name, source_path, status, comment, last_updated)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (source_path)
        DO UPDATE SET status = EXCLUDED.status,
                      comment = EXCLUDED.comment,
                      last_updated = NOW();
    """
    
    cur.execute(sql, (image_name, source_path, status, comment))
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"💾 Updated DB status: {image_name} ({source_path}) → {status}")

def main():
    input_file = Path("2025dtm.txt")
    if not input_file.exists():
        logger.error(f"❌ File not found: {input_file}")
        return
    paths = [line.strip() for line in input_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    logger.info(f"📄 Loaded {len(paths)} paths from {input_file}")

    # open a single RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    for path_str in paths:
        path = Path(path_str)
        areal_dir = path.parent.parent
        preprocessed_checks = [
            ("fangstgrop", areal_dir / "preprocessed_10cm"),
            ("kolbotten", areal_dir / "preprocessed_20cm")
        ]
        for model_name, folder in preprocessed_checks:
            if folder.exists() and folder.is_dir() and any(folder.iterdir()):
                safe_publish("inference", {"path": str(path), "inference_path": str(folder)})
            else:
                update_db_status(str(path))
                logger.warning(f"⚠️ Missing folder for {model_name}: {folder}, reset its status")

    connection.close()
    logger.info("✅ All paths processed")

if __name__ == "__main__":
    main()
