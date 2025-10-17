# Use slim Python image
FROM python:3.10-slim-bullseye

# Set working directory
WORKDIR /app

# Allow specifying requirements at build time
ARG REQUIREMENTS_FILE=requirements.txt
COPY ${REQUIREMENTS_FILE} ./

# Install Python dependencies
RUN pip install --no-cache-dir -r ${REQUIREMENTS_FILE}

# Copy application code
COPY . .

# Ensure logs directory exists
RUN mkdir -p logs

CMD ["python3", "--version"]
