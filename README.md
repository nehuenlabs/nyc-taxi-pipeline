# NYC Taxi Mini Pipeline

[![CI](https://github.com/nehuenlabs/nyc-taxi-pipeline/workflows/CI/badge.svg)](https://github.com/nehuenlabs/nyc-taxi-pipeline/actions)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A data pipeline for processing NYC Taxi data using PySpark, loading into a dimensional model, and infrastructure provisioned with Terraform on GCP.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         NYC TAXI PIPELINE                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────────────┐   │
│   │  Raw    │────▶│ Bronze  │────▶│  Gold   │────▶│   PostgreSQL    │   │
│   │ (NYC    │     │ (Raw +  │     │ (Star   │     │   / BigQuery    │   │
│   │  TLC)   │     │  Meta)  │     │ Schema) │     │                 │   │
│   └─────────┘     └─────────┘     └─────────┘     └─────────────────┘   │
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    Development: Docker                           │  │
│   │                    Production:  GCP (Terraform)                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## 📋 Table of Contents

- [Features](#-features)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Project Structure](#-project-structure)
- [Data Model](#-data-model)
- [Pipeline Jobs](#-pipeline-jobs)
- [Configuration](#-configuration)
- [Terraform Infrastructure](#-terraform-infrastructure)
- [Testing](#-testing)
- [CI/CD](#-cicd)
- [Historical Data Strategy](#-historical-data-strategy)
- [Limitations & Future Improvements](#-limitations--future-improvements)

## ✨ Features

- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **Dimensional Modeling**: Star schema with fact and dimension tables
- **Environment Agnostic**: Same code runs locally (Docker) and on GCP
- **Infrastructure as Code**: Terraform modules for GCP resources
- **Historical Data Support**: Backfill and re-process specific months
- **Idempotent Processing**: Partition overwrite prevents duplicates
- **Comprehensive Testing**: Unit and integration tests with pytest
- **CI/CD Pipeline**: GitHub Actions for automated validation

## 📦 Prerequisites

- **Docker** & **Docker Compose** (v2.0+)
- **Python** 3.10+ (for local development)
- **Make** (optional, for convenience commands)
- **Terraform** 1.0+ (for GCP infrastructure)

For GCP deployment (optional):
- GCP Account with billing enabled
- `gcloud` CLI configured

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/marcelo/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

# Initial setup (creates directories, installs dependencies)
make setup
```

### 2. Start Services

```bash
# Start Spark + PostgreSQL
make start

# Verify services are running
make status
```

### 3. Run the Pipeline

```bash
# Download sample data (January 2023)
make download-data MONTHS=2023-01

# Run full pipeline
make run-full MONTHS=2023-01
```

### 4. Verify Results

- **Spark UI**: http://localhost:8081
- **Adminer (DB UI)**: http://localhost:8080
  - System: PostgreSQL
  - Server: postgres
  - User: pipeline
  - Password: pipeline123
  - Database: nyctaxi

## 📁 Project Structure

```
nyc-taxi-pipeline/
├── src/                    # Source code
│   ├── jobs/              # PySpark jobs
│   ├── models/            # Data schemas & dimensional model
│   ├── utils/             # Configuration & utilities
│   └── quality/           # Data quality validators
├── tests/                 # Test suite
│   ├── unit/             # Unit tests
│   └── integration/      # Integration tests
├── terraform/             # GCP infrastructure
│   └── modules/          # Terraform modules (GCS, IAM, BigQuery)
├── docker/               # Docker configuration
├── sql/                  # SQL scripts
│   ├── postgres/        # PostgreSQL DDL
│   └── bigquery/        # BigQuery DDL
├── scripts/              # Utility scripts
├── data/                 # Local data (gitignored)
└── docs/                 # Documentation
```

## 📊 Data Model

### Star Schema

```
                    ┌─────────────────┐
                    │    dim_date     │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  dim_location   │──────────┼──────────│   dim_vendor    │
└─────────────────┘          │          └─────────────────┘
                             │
                    ┌────────┴────────┐
                    │   fact_trips    │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  dim_ratecode   │──────────┼──────────│ dim_payment_type│
└─────────────────┘          │          └─────────────────┘
                             │
                    ┌────────┴────────┐
                    │    dim_time     │
                    └─────────────────┘
```

## 🔧 Pipeline Jobs

### Job 1: Bronze Ingestion

```bash
make run-bronze MONTHS=2023-01
```

- Downloads raw Parquet data from NYC TLC
- Adds metadata columns (ingestion timestamp, source file)
- Partitions by year/month
- Stores in Bronze layer

### Job 2: Dimensional Transform

```bash
make run-transform MONTHS=2023-01
```

- Reads from Bronze layer
- Cleans and validates data
- Joins with zone lookup
- Creates fact and dimension tables
- Loads into PostgreSQL

## ⚙️ Configuration

Environment variables (`.env`):

| Variable | Description | Default |
|----------|-------------|---------|
| `PIPELINE_ENV` | Environment (local/gcp) | `local` |
| `POSTGRES_HOST` | PostgreSQL host | `postgres` |
| `POSTGRES_DB` | Database name | `nyctaxi` |
| `GCS_BUCKET` | GCS bucket (GCP only) | - |
| `GCP_PROJECT_ID` | GCP project (GCP only) | - |

## 🏗️ Terraform Infrastructure

### Structure

```
terraform/
├── main.tf              # GCS buckets, Service Account, IAM bindings
├── variables.tf         # Input variables
├── outputs.tf          # Output values
└── terraform.tfvars    # Your configuration (not committed)
```

### Configuration

1. Copy the example file:
```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

2. Edit `terraform.tfvars` with your GCP project:
```hcl
project_id  = "your-gcp-project-id"
region      = "us-central1"
environment = "dev"
```

### Deploy

```bash
# Authenticate with GCP
gcloud auth application-default login

# Enable required APIs
gcloud services enable storage.googleapis.com iam.googleapis.com

# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Apply (creates resources)
terraform apply

# Destroy when done (optional)
terraform destroy
```

### Resources Created

| Resource | Description |
|----------|-------------|
| `google_storage_bucket.bronze` | GCS bucket for raw/bronze data |
| `google_storage_bucket.gold` | GCS bucket for curated/gold data |
| `google_service_account.pipeline` | Service account for pipeline |
| `google_storage_bucket_iam_member` | IAM bindings for bucket access |

## 🧪 Testing

```bash
# Run all tests
make test

# Run unit tests only
pytest tests/unit/ -v

# Run integration tests (requires PySpark)
pytest tests/integration/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

### Test Structure

```
tests/
├── unit/                    # Fast, isolated tests
│   ├── test_schemas.py     # Schema validation
│   └── test_config.py      # Configuration tests
├── integration/            # End-to-end tests
│   └── test_pipeline.py    # Full pipeline tests with Spark
└── conftest.py             # Shared fixtures
```

## 📊 Monitoring & Alerting

The pipeline includes production-grade monitoring capabilities:

### Quick Commands

```bash
# Full health report (requires Docker running)
make monitor

# Demo the PipelineMonitor (no DB required)
make monitor-demo

# Local monitoring (requires POSTGRES_HOST=localhost)
make monitor-local
```

### Structured Logging

```python
from src.monitoring import get_logger

logger = get_logger(__name__)
logger.info("Processing started", extra={"extra_fields": {"records": 1000}})
```

Outputs JSON logs compatible with CloudWatch, Stackdriver, ELK:
```json
{"timestamp": "2023-01-15T10:30:00Z", "level": "INFO", "message": "Processing started", "records": 1000}
```

### Pipeline Metrics

```python
from src.monitoring import PipelineMonitor

monitor = PipelineMonitor("bronze_ingestion")

with monitor.track_stage("download") as stage:
    data = download_data()
    stage.records_out = len(data)

with monitor.track_stage("transform") as stage:
    stage.records_in = len(data)
    result = transform(data)
    stage.records_out = len(result)
    stage.records_rejected = len(data) - len(result)

monitor.report()  # Prints execution summary
```

### Automatic Alerting

Alerts are triggered automatically when:
- **WARNING**: Rejection rate > 5%
- **CRITICAL**: Rejection rate > 20%
- **CRITICAL**: Stage failure/exception

### Data Quality Checks

```python
from src.monitoring import DataQualityMonitor

dq = DataQualityMonitor(df, "trips_data")
dq.check_nulls(["fare_amount", "trip_distance"])
dq.check_range("fare_amount", min_val=0, max_val=500)
dq.check_uniqueness("trip_id")

report = dq.get_report()
```

## 🔄 CI/CD

GitHub Actions workflow runs on every push:

1. **Lint**: flake8, black, isort
2. **Unit Tests**: pytest for schemas, config
3. **Integration Tests**: Full Spark pipeline tests
4. **Terraform**: fmt + validate
5. **Docker**: Build image
6. **Security**: Dependency vulnerability scan

### Pipeline Status

All jobs run automatically on push to `main` or `develop` branches.

## 📅 Historical Data Strategy

### Processing Multiple Months

```bash
make run-full MONTHS="2023-01 2023-02 2023-03"
```

### Backfill Specific Month

```bash
make run-backfill MONTHS=2023-02
```

### Idempotency

- Uses partition overwrite mode
- Re-running for a month replaces only that partition
- No duplicates, no data corruption

## ⚠️ Considerations

### Current Limitations

- Single-node Spark (Docker) for development
- No real-time streaming (batch only)
- Alerting is log-based (extensible to Slack/PagerDuty)

### Implemented ✅

- [x] Structured JSON logging for log aggregators
- [x] Pipeline monitoring with metrics
- [x] Automatic alerting on data quality issues
- [x] Unit and integration tests
- [x] CI/CD with GitHub Actions
- [x] Infrastructure as Code (Terraform)


