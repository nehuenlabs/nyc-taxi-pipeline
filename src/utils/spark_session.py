"""
Spark Session Factory
=====================

Creates and configures SparkSession for different environments.

Usage:
    from src.utils.config import PipelineConfig
    from src.utils.spark_session import create_spark_session
    
    config = PipelineConfig.load()
    spark = create_spark_session(config)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from src.utils.config import PipelineConfig, SparkConfig

logger = logging.getLogger(__name__)


def create_spark_session(
    config: PipelineConfig,
    app_name: Optional[str] = None,
) -> SparkSession:
    """
    Create a SparkSession configured for the current environment.
    
    Args:
        config: Pipeline configuration object
        app_name: Optional override for application name
    
    Returns:
        Configured SparkSession
    
    Example:
        config = PipelineConfig.load()
        spark = create_spark_session(config)
        
        df = spark.read.parquet("path/to/data")
    """
    spark_config = config.spark
    
    # Use provided app_name or default from config
    name = app_name or spark_config.app_name
    
    logger.info(f"Creating SparkSession: {name}")
    logger.info(f"Environment: {config.environment.value}")
    logger.info(f"Master: {spark_config.master}")
    
    # Build SparkSession
    builder = (
        SparkSession.builder
        .appName(name)
        .master(spark_config.master)
        .config("spark.driver.memory", spark_config.driver_memory)
        .config("spark.executor.memory", spark_config.executor_memory)
    )
    
    # Add extra configurations
    for key, value in spark_config.extra_configs.items():
        builder = builder.config(key, value)
    
    # Add JDBC driver for PostgreSQL
    builder = builder.config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.6.0"
    )
    
    # Environment-specific configurations
    if config.is_local:
        builder = _configure_local(builder, config)
    else:
        builder = _configure_gcp(builder, config)
    
    # Create session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel(config.log_level)
    
    logger.info(f"SparkSession created successfully")
    logger.info(f"Spark version: {spark.version}")
    
    return spark


def _configure_local(
    builder: SparkSession.Builder,
    config: PipelineConfig,
) -> SparkSession.Builder:
    """
    Apply local-specific Spark configurations.
    
    Args:
        builder: SparkSession builder
        config: Pipeline configuration
    
    Returns:
        Configured builder
    """
    return (
        builder
        # Local mode optimizations
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        # Enable Hive support for complex queries
        .config("spark.sql.catalogImplementation", "in-memory")
        # Memory settings for local
        .config("spark.driver.maxResultSize", "1g")
    )


def _configure_gcp(
    builder: SparkSession.Builder,
    config: PipelineConfig,
) -> SparkSession.Builder:
    """
    Apply GCP-specific Spark configurations.
    
    Args:
        builder: SparkSession builder
        config: Pipeline configuration
    
    Returns:
        Configured builder
    """
    builder = (
        builder
        # GCS connector
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        # Performance settings
        .config("spark.sql.shuffle.partitions", "200")
    )
    
    # BigQuery connector if configured
    if config.bigquery:
        builder = builder.config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.6.0,"
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2"
        )
        builder = builder.config("temporaryGcsBucket", f"{config.storage.bronze_path}/temp")
    
    return builder


def get_or_create_spark_session(
    config: Optional[PipelineConfig] = None,
    app_name: str = "NYCTaxiPipeline",
) -> SparkSession:
    """
    Get existing SparkSession or create a new one.
    
    This is useful when you want to reuse an existing session
    in interactive environments (notebooks, shell).
    
    Args:
        config: Optional pipeline configuration. If not provided,
                will load from environment.
        app_name: Application name for new session
    
    Returns:
        SparkSession (existing or new)
    """
    # Try to get existing session
    existing = SparkSession.getActiveSession()
    if existing is not None:
        logger.info("Using existing SparkSession")
        return existing
    
    # Create new session
    if config is None:
        from src.utils.config import PipelineConfig
        config = PipelineConfig.load()
    
    return create_spark_session(config, app_name)


def stop_spark_session(spark: SparkSession) -> None:
    """
    Safely stop a SparkSession.
    
    Args:
        spark: SparkSession to stop
    """
    if spark is not None:
        logger.info("Stopping SparkSession")
        spark.stop()
        logger.info("SparkSession stopped")


class SparkSessionManager:
    """
    Context manager for SparkSession lifecycle.
    
    Usage:
        config = PipelineConfig.load()
        
        with SparkSessionManager(config) as spark:
            df = spark.read.parquet("path/to/data")
            # ... do work ...
        # Session automatically stopped
    """
    
    def __init__(
        self,
        config: PipelineConfig,
        app_name: Optional[str] = None,
    ):
        self.config = config
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
    
    def __enter__(self) -> SparkSession:
        self.spark = create_spark_session(self.config, self.app_name)
        return self.spark
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.spark is not None:
            stop_spark_session(self.spark)
        return False  # Don't suppress exceptions
