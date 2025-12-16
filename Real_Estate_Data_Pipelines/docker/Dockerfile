# docker/Dockerfile
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    wget \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/dagster:/opt/dagster/app:/opt/dagster/src

# Create directories
RUN mkdir -p /opt/dagster/app \
             /opt/dagster/src \
             /opt/dagster/raw_data \
             /opt/dagster/logs \
             /opt/dagster/dagster_home

# Set working directory
WORKDIR /opt/dagster/app

# Copy requirements
COPY requirements.txt /opt/dagster/app/requirements.txt

# Upgrade pip and install dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Expose ports
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:3000/server_info || exit 1

# Default command
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/app/workspace.yaml"]