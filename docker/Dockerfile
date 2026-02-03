# ============================================
# NYC Taxi Pipeline - Dockerfile
# ============================================

FROM python:3.11-slim

# Prevent Python from writing pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Java (required for PySpark)
    default-jre-headless \
    # PostgreSQL client libraries
    libpq-dev \
    # Network tools
    curl \
    wget \
    # Build essentials for Python packages
    gcc \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy requirements first (for layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy source code and config
COPY src/ ./src/
COPY pyproject.toml .

# Install the package
RUN pip install --no-cache-dir -e .

# Create data directories
RUN mkdir -p /data/raw /data/bronze /data/silver /data/gold

# Set environment variables
ENV PIPELINE_ENV=local
ENV PYTHONPATH=/app
ENV DATA_PATH=/data

# Default command - show pipeline info
CMD ["python", "-m", "src.cli", "info"]
