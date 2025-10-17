import time
import logging
import json
import sys
from pathlib import Path
import pika
import os
from db_utils import PreprocessRepository, Status

# --- Logging ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
Path("logs").mkdir(exist_ok=True)
formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler("logs/publisher.log", mode="a")
fh.setFormatter(formatter)
logger.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

repo = PreprocessRepository()
QUEUE_POLL_INTERVAL = 5

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))

def get_rabbit_connection(retries=5, delay=5):
    retries = 5
    delay = 5
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

def main():
    while True:
        job = repo.get_next_unprocessed()
        if job:
            path = Path(job["source_path"])
            repo.update_status(path, Status.QUEUED)
            safe_publish("preprocess", {"path": str(path)})
        else:
            time.sleep(QUEUE_POLL_INTERVAL)

if __name__ == "__main__":
    main()
