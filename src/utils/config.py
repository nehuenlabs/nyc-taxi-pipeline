"""
Configuration Management
========================

Environment-agnostic configuration for the NYC Taxi Pipeline.
Supports both local (Docker) and GCP environments.

Usage:
    from src.utils.config import PipelineConfig
    
    config = PipelineConfig.load()
    print(config.environment)  # "local" or "gcp"
    print(config.storage.bronze_path)  # "./data/bronze" or "gs://bucket/bronze"
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


class Environment(str, Enum):
    """Pipeline execution environment."""
    LOCAL = "local"
    GCP = "gcp"


@dataclass
class StorageConfig:
    """
    Storage configuration for data lake layers.
    
    Supports:
        - Local filesystem (Docker)
        - Google Cloud Storage (GCP)
    """
    raw_path: str
    bronze_path: str
    silver_path: str
    gold_path: str
    
    @classmethod
    def for_local(cls, base_path: str = "/data") -> StorageConfig:
        """Create configuration for local filesystem."""
        return cls(
            raw_path=f"{base_path}/raw",
            bronze_path=f"{base_path}/bronze",
            silver_path=f"{base_path}/silver",
            gold_path=f"{base_path}/gold",
        )
    
    @classmethod
    def for_gcp(cls, bucket_name: str) -> StorageConfig:
        """Create configuration for Google Cloud Storage."""
        return cls(
            raw_path=f"gs://{bucket_name}/raw",
            bronze_path=f"gs://{bucket_name}/bronze",
            silver_path=f"gs://{bucket_name}/silver",
            gold_path=f"gs://{bucket_name}/gold",
        )
    
    def get_trips_bronze_path(self) -> str:
        """Get the path for trips data in bronze layer."""
        return f"{self.bronze_path}/trips"
    
    def get_zones_bronze_path(self) -> str:
        """Get the path for zone lookup data in bronze layer."""
        return f"{self.bronze_path}/zones"
    
    def get_trips_gold_path(self) -> str:
        """Get the path for fact_trips in gold layer."""
        return f"{self.gold_path}/fact_trips"
    
    def get_dimensions_gold_path(self) -> str:
        """Get the base path for dimensions in gold layer."""
        return f"{self.gold_path}/dimensions"


@dataclass
class PostgresConfig:
    """PostgreSQL database configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = "nyc_taxi_dw"
    
    @classmethod
    def from_env(cls) -> PostgresConfig:
        """Create configuration from environment variables."""
        return cls(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "nyctaxi"),
            user=os.getenv("POSTGRES_USER", "pipeline"),
            password=os.getenv("POSTGRES_PASSWORD", "pipeline123"),
            schema=os.getenv("POSTGRES_SCHEMA", "nyc_taxi_dw"),
        )
    
    @property
    def jdbc_url(self) -> str:
        """Get JDBC connection URL for Spark."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    @property
    def connection_properties(self) -> dict:
        """Get connection properties for Spark JDBC."""
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }
    
    @property
    def sqlalchemy_url(self) -> str:
        """Get SQLAlchemy connection URL."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class BigQueryConfig:
    """BigQuery configuration (for GCP environment)."""
    project_id: str
    dataset: str
    location: str = "US"
    
    @classmethod
    def from_env(cls) -> BigQueryConfig:
        """Create configuration from environment variables."""
        return cls(
            project_id=os.getenv("GCP_PROJECT_ID", ""),
            dataset=os.getenv("BQ_DATASET", "nyc_taxi_dw"),
            location=os.getenv("BQ_LOCATION", "US"),
        )
    
    @property
    def full_dataset_id(self) -> str:
        """Get fully qualified dataset ID."""
        return f"{self.project_id}.{self.dataset}"


@dataclass
class SparkConfig:
    """Apache Spark configuration."""
    master: str
    app_name: str
    driver_memory: str
    executor_memory: str
    
    # Additional Spark configurations
    extra_configs: dict = field(default_factory=dict)
    
    @classmethod
    def for_local(cls, app_name: str = "NYCTaxiPipeline") -> SparkConfig:
        """Create configuration for local Spark (Docker)."""
        return cls(
            master=os.getenv("SPARK_MASTER", "spark://spark-master:7077"),
            app_name=app_name,
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "1g"),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "1g"),
            extra_configs={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.parquet.compression.codec": "snappy",
                "spark.sql.session.timeZone": "UTC",
            },
        )
    
    @classmethod
    def for_gcp(cls, app_name: str = "NYCTaxiPipeline") -> SparkConfig:
        """Create configuration for GCP Dataproc."""
        return cls(
            master="yarn",
            app_name=app_name,
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
            extra_configs={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.parquet.compression.codec": "snappy",
                "spark.sql.session.timeZone": "UTC",
                # GCS connector
                "spark.hadoop.google.cloud.auth.service.account.enable": "true",
            },
        )


@dataclass
class PipelineConfig:
    """
    Main configuration class for the NYC Taxi Pipeline.
    
    Aggregates all configuration components and provides
    factory methods for different environments.
    
    Usage:
        # Load from environment
        config = PipelineConfig.load()
        
        # Access components
        print(config.storage.bronze_path)
        print(config.database.jdbc_url)
    """
    environment: Environment
    storage: StorageConfig
    spark: SparkConfig
    postgres: PostgresConfig
    bigquery: Optional[BigQueryConfig] = None
    
    # Logging configuration
    log_level: str = "INFO"
    
    @classmethod
    def load(cls) -> PipelineConfig:
        """
        Load configuration from environment variables.
        
        The PIPELINE_ENV variable determines which environment
        configuration to load ("local" or "gcp").
        """
        env_str = os.getenv("PIPELINE_ENV", "local").lower()
        
        try:
            environment = Environment(env_str)
        except ValueError:
            raise ValueError(
                f"Invalid PIPELINE_ENV: '{env_str}'. "
                f"Must be one of: {[e.value for e in Environment]}"
            )
        
        if environment == Environment.LOCAL:
            return cls._load_local()
        else:
            return cls._load_gcp()
    
    @classmethod
    def _load_local(cls) -> PipelineConfig:
        """Load configuration for local Docker environment."""
        data_path = os.getenv("DATA_PATH", "/data")
        
        return cls(
            environment=Environment.LOCAL,
            storage=StorageConfig.for_local(data_path),
            spark=SparkConfig.for_local(),
            postgres=PostgresConfig.from_env(),
            bigquery=None,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
    
    @classmethod
    def _load_gcp(cls) -> PipelineConfig:
        """Load configuration for GCP environment."""
        bucket_name = os.getenv("GCS_BUCKET")
        
        if not bucket_name:
            raise ValueError("GCS_BUCKET environment variable is required for GCP environment")
        
        return cls(
            environment=Environment.GCP,
            storage=StorageConfig.for_gcp(bucket_name),
            spark=SparkConfig.for_gcp(),
            postgres=PostgresConfig.from_env(),  # Can still use Postgres in GCP
            bigquery=BigQueryConfig.from_env(),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
    
    @property
    def is_local(self) -> bool:
        """Check if running in local environment."""
        return self.environment == Environment.LOCAL
    
    @property
    def is_gcp(self) -> bool:
        """Check if running in GCP environment."""
        return self.environment == Environment.GCP
    
    @property
    def database(self) -> PostgresConfig:
        """
        Get the primary database configuration.
        
        For local: PostgreSQL
        For GCP: PostgreSQL (BigQuery available via self.bigquery)
        """
        return self.postgres


# ============================================
# NYC Taxi Data URLs
# ============================================

NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TRIP_DATA_URL_TEMPLATE = f"{NYC_TLC_BASE_URL}/trip-data/yellow_tripdata_{{year_month}}.parquet"
ZONE_LOOKUP_URL = f"{NYC_TLC_BASE_URL}/misc/taxi_zone_lookup.csv"


def get_trip_data_url(year_month: str) -> str:
    """
    Get the URL for NYC TLC trip data.
    
    Args:
        year_month: Month in format "YYYY-MM" (e.g., "2023-01")
    
    Returns:
        URL string for the parquet file
    """
    return TRIP_DATA_URL_TEMPLATE.format(year_month=year_month)


def get_zone_lookup_url() -> str:
    """Get the URL for taxi zone lookup CSV."""
    return ZONE_LOOKUP_URL
