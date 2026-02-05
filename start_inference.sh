#!/bin/sh
mkdir -p /app/temp

# Wait for RabbitMQ
while ! python3 -c 'import socket; exit(0) if socket.socket().connect_ex(("rabbitmq",5672))==0 else exit(1)'; do
  echo "Waiting for RabbitMQ..."
  sleep 2
done

pip install pika psycopg2-binary
python3 inference_worker.py