# Dockerfile
FROM python:3.10-slim

# Set workdir inside container
WORKDIR /app

# Install system dependencies (adjust if needed for geospatial libs)
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY . .

# Default command (overridden in docker-compose)
CMD ["python", "publisher.py"]
