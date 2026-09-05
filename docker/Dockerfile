# docker/Dockerfile
FROM python:3.11-slim

# Install Astral uv binary from official image (10-100x faster than pip)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

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
ENV DAGSTER_HOME=/opt/dagster/dagster_home \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/dagster/real_estate/src:/opt/dagster/real_estate \
    UV_SYSTEM_PYTHON=1

# Create directories
RUN mkdir -p /opt/dagster/real_estate/src/real_estate/pipelines \
             /opt/dagster/real_estate/src \
             /opt/dagster/real_estate/data/raw \
             /opt/dagster/real_estate/logs \
             /opt/dagster/dagster_home

# Set working directory
WORKDIR /opt/dagster/real_estate

# Copy requirements
COPY requirements.txt /opt/dagster/real_estate/requirements.txt

# Install dependencies using uv
RUN uv pip install --system -r requirements.txt

# Expose ports
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:3000/server_info || exit 1

# Default command
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/real_estate/src/real_estate/pipelines/workspace.yaml"]