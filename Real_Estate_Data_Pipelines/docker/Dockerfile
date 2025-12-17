# docker/Dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    wget \
    build-essential \
    python3-dev \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/dagster:/opt/dagster/app:/opt/dagster/src

# Create directories
RUN mkdir -p /opt/dagster/Real_Estate_Data_Pipelines/app \
             /opt/dagster/Real_Estate_Data_Pipelines/src \
             /opt/dagster/Real_Estate_Data_Pipelines/raw_data \
             /opt/dagster/Real_Estate_Data_Pipelines/logs \
             /opt/dagster/dagster_home

# Set working directory
WORKDIR /opt/dagster/Real_Estate_Data_Pipelines/app

# Copy requirements
COPY requirements.txt /opt/dagster/Real_Estate_Data_Pipelines/app/requirements.txt

# Upgrade pip and install dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir --prefer-binary -r requirements.txt

# Expose ports
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:3000/server_info || exit 1

# Default command
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/app/workspace.yaml"]