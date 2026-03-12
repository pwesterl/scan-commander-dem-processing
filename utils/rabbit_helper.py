import os
import json
import time
import logging
import pika
from typing import Any

class RabbitMQHelper:
    def __init__(self, host: str | None = None, port: int | None = None, logger: logging.Logger | None = None):
        self.host = host or os.getenv("RABBITMQ_HOST", "localhost")
        self.port = port or int(os.getenv("RABBITMQ_PORT", 5672))
        self.logger = logger or logging.getLogger(__name__)
        
        # Silence pika internal _produce stack traces
        logging.getLogger("pika.adapters.utils.io_services_utils").setLevel(logging.WARNING)

    def get_connection(self, retries: int = 5, delay: int = 5) -> pika.BlockingConnection:
        for attempt in range(1, retries + 1):
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, port=self.port)
                )
                return connection
            except (pika.exceptions.AMQPConnectionError, ConnectionResetError, pika.exceptions.StreamLostError) as e:
                self.logger.warning(f"RabbitMQ connection failed ({attempt}/{retries}): {e}")
                time.sleep(delay)
        raise RuntimeError("Could not connect to RabbitMQ after multiple retries")

    def safe_publish(self, queue_name: str, message: Any, retries: int = 5, delay: int = 5):
        attempt = 0
        while True:
            attempt += 1
            try:
                connection = self.get_connection(retries=retries, delay=delay)
                channel = connection.channel()
                channel.queue_declare(queue=queue_name, durable=True)
                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=json.dumps(message),
                    properties=pika.BasicProperties(delivery_mode=2),  # persistent messages
                )
                connection.close()
                self.logger.info(f"Published job to {queue_name}: {message}")
                break
            except (pika.exceptions.AMQPConnectionError, ConnectionResetError, pika.exceptions.StreamLostError) as e:
                self.logger.warning(f"Publish attempt {attempt} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)

    @staticmethod
    def safe_ack(ch: pika.adapters.blocking_connection.BlockingChannel, delivery_tag: int, logger: logging.Logger | None = None):
        try:
            ch.basic_ack(delivery_tag=delivery_tag)
        except (pika.exceptions.AMQPConnectionError, ConnectionResetError, pika.exceptions.StreamLostError) as e:
            if logger:
                logger.warning("Ack failed due to lost stream; message will be requeued automatically")