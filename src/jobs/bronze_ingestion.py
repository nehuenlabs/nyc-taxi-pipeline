"""
Bronze Ingestion Job
====================

Ingests raw NYC Taxi data into the Bronze layer.

Bronze layer characteristics:
    - Raw data as-is from source
    - Added metadata columns (_source_file, _ingestion_ts, _year, _month)
    - Partitioned by year/month for efficient querying
    - Idempotent: re-running overwrites the same partition

Usage:
    python -m src.jobs.bronze_ingestion --months 2023-01 2023-02
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from urllib.request import urlretrieve

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.models.schemas import ZONE_LOOKUP_SCHEMA
from src.utils.config import PipelineConfig

logger = logging.getLogger(__name__)


# NYC TLC Data URLs
NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TRIP_DATA_URL = f"{NYC_TLC_BASE_URL}/trip-data/yellow_tripdata_{{year_month}}.parquet"
ZONE_LOOKUP_URL = f"{NYC_TLC_BASE_URL}/misc/taxi_zone_lookup.csv"


def download_trip_data(
    year_month: str,
    output_dir: str,
    force: bool = False,
) -> str:
    """
    Download trip data for a specific month.
    
    Args:
        year_month: Month in format "YYYY-MM"
        output_dir: Directory to save the file
        force: If True, re-download even if file exists
    
    Returns:
        Path to the downloaded file
    """
    filename = f"yellow_tripdata_{year_month}.parquet"
    output_path = Path(output_dir) / filename
    
    # Create directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Skip if already exists
    if output_path.exists() and not force:
        logger.info(f"File already exists, skipping: {output_path}")
        return str(output_path)
    
    # Download
    url = TRIP_DATA_URL.format(year_month=year_month)
    logger.info(f"Downloading: {url}")
    logger.info(f"Destination: {output_path}")
    
    try:
        urlretrieve(url, output_path)
        logger.info(f"Downloaded successfully: {output_path}")
        return str(output_path)
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        raise


def download_zone_lookup(
    output_dir: str,
    force: bool = False,
) -> str:
    """
    Download taxi zone lookup file.
    
    Args:
        output_dir: Directory to save the file
        force: If True, re-download even if file exists
    
    Returns:
        Path to the downloaded file
    """
    filename = "taxi_zone_lookup.csv"
    output_path = Path(output_dir) / filename
    
    # Create directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Skip if already exists
    if output_path.exists() and not force:
        logger.info(f"File already exists, skipping: {output_path}")
        return str(output_path)
    
    # Download
    logger.info(f"Downloading: {ZONE_LOOKUP_URL}")
    logger.info(f"Destination: {output_path}")
    
    try:
        urlretrieve(ZONE_LOOKUP_URL, output_path)
        logger.info(f"Downloaded successfully: {output_path}")
        return str(output_path)
    except Exception as e:
        logger.error(f"Failed to download zone lookup: {e}")
        raise


def read_raw_trips(
    spark: SparkSession,
    file_path: str,
) -> DataFrame:
    """
    Read raw trip data from parquet file.
    
    Args:
        spark: SparkSession
        file_path: Path to parquet file
    
    Returns:
        DataFrame with raw trip data
    """
    logger.info(f"Reading raw trips from: {file_path}")
    
    # Read without schema enforcement - let Spark infer from Parquet metadata
    # This avoids type mismatch issues (INT64 vs INT32, etc.)
    df = spark.read.parquet(file_path)
    
    row_count = df.count()
    logger.info(f"Read {row_count:,} rows from {file_path}")
    
    return df


def read_zone_lookup(
    spark: SparkSession,
    file_path: str,
) -> DataFrame:
    """
    Read zone lookup from CSV file.
    
    Args:
        spark: SparkSession
        file_path: Path to CSV file
    
    Returns:
        DataFrame with zone lookup data
    """
    logger.info(f"Reading zone lookup from: {file_path}")
    
    df = (
        spark.read
        .option("header", "true")
        .schema(ZONE_LOOKUP_SCHEMA)
        .csv(file_path)
    )
    
    row_count = df.count()
    logger.info(f"Read {row_count:,} zones from {file_path}")
    
    return df


def add_bronze_metadata(
    df: DataFrame,
    source_file: str,
    year_month: str,
) -> DataFrame:
    """
    Add metadata columns for bronze layer.
    
    Args:
        df: Raw DataFrame
        source_file: Source file name
        year_month: Source month (YYYY-MM)
    
    Returns:
        DataFrame with metadata columns
    """
    # Parse year and month
    year = int(year_month.split("-")[0])
    month = int(year_month.split("-")[1])
    
    return (
        df
        # Cast columns to appropriate types (source uses INT64/Long)
        .withColumn("passenger_count", 
            F.col("passenger_count").cast("int"))
        .withColumn("VendorID",
            F.col("VendorID").cast("int"))
        .withColumn("PULocationID",
            F.col("PULocationID").cast("int"))
        .withColumn("DOLocationID",
            F.col("DOLocationID").cast("int"))
        .withColumn("RatecodeID", 
            F.col("RatecodeID").cast("int"))
        .withColumn("payment_type",
            F.col("payment_type").cast("int"))
        # Add metadata
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_month", F.lit(year_month))
        .withColumn("_ingestion_ts", F.current_timestamp())
        # Add partition columns
        .withColumn("_year", F.lit(year))
        .withColumn("_month", F.lit(month))
    )


def write_bronze_trips(
    df: DataFrame,
    output_path: str,
    mode: str = "overwrite",
) -> None:
    """
    Write trips to bronze layer with partitioning.
    
    Args:
        df: DataFrame with metadata
        output_path: Output directory path
        mode: Write mode ("overwrite" or "append")
    """
    logger.info(f"Writing bronze trips to: {output_path}")
    logger.info(f"Mode: {mode}")
    
    # Get partition values for logging
    partitions = df.select("_year", "_month").distinct().collect()
    for p in partitions:
        logger.info(f"Writing partition: _year={p._year}, _month={p._month}")
    
    (
        df.write
        .mode(mode)
        .partitionBy("_year", "_month")
        .parquet(output_path)
    )
    
    logger.info(f"Bronze trips written successfully")


def write_bronze_zones(
    df: DataFrame,
    output_path: str,
) -> None:
    """
    Write zone lookup to bronze layer.
    
    Args:
        df: Zone lookup DataFrame
        output_path: Output file path
    """
    logger.info(f"Writing bronze zones to: {output_path}")
    
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
    
    logger.info(f"Bronze zones written successfully")


def run_bronze_ingestion(
    config: PipelineConfig,
    spark: SparkSession,
    months: List[str],
    download: bool = True,
    force_download: bool = False,
) -> dict:
    """
    Run the bronze ingestion job.
    
    Args:
        config: Pipeline configuration
        spark: SparkSession
        months: List of months to process (format: "YYYY-MM")
        download: If True, download data from NYC TLC
        force_download: If True, re-download even if files exist
    
    Returns:
        Dictionary with job statistics
    """
    logger.info("=" * 60)
    logger.info("BRONZE INGESTION JOB")
    logger.info("=" * 60)
    logger.info(f"Environment: {config.environment.value}")
    logger.info(f"Months to process: {months}")
    logger.info(f"Raw path: {config.storage.raw_path}")
    logger.info(f"Bronze path: {config.storage.bronze_path}")
    
    stats = {
        "months_processed": 0,
        "total_rows": 0,
        "zones_loaded": 0,
    }
    
    # Download and process zone lookup (once)
    if download:
        zone_file = download_zone_lookup(
            config.storage.raw_path,
            force=force_download
        )
    else:
        zone_file = f"{config.storage.raw_path}/taxi_zone_lookup.csv"
    
    # Read and write zones to bronze
    zones_df = read_zone_lookup(spark, zone_file)
    zones_bronze_path = f"{config.storage.bronze_path}/zones"
    write_bronze_zones(zones_df, zones_bronze_path)
    stats["zones_loaded"] = zones_df.count()
    
    # Process each month
    trips_bronze_path = f"{config.storage.bronze_path}/trips"
    
    for year_month in months:
        logger.info("-" * 40)
        logger.info(f"Processing month: {year_month}")
        
        # Download if needed
        if download:
            raw_file = download_trip_data(
                year_month,
                config.storage.raw_path,
                force=force_download
            )
        else:
            raw_file = f"{config.storage.raw_path}/yellow_tripdata_{year_month}.parquet"
        
        # Read raw data
        raw_df = read_raw_trips(spark, raw_file)
        
        # Add metadata
        bronze_df = add_bronze_metadata(
            raw_df,
            source_file=Path(raw_file).name,
            year_month=year_month
        )
        
        # Write to bronze (partition overwrite)
        write_bronze_trips(bronze_df, trips_bronze_path, mode="overwrite")
        
        # Update stats
        row_count = bronze_df.count()
        stats["months_processed"] += 1
        stats["total_rows"] += row_count
        
        logger.info(f"Processed {row_count:,} rows for {year_month}")
    
    logger.info("=" * 60)
    logger.info("BRONZE INGESTION COMPLETE")
    logger.info(f"Months processed: {stats['months_processed']}")
    logger.info(f"Total rows: {stats['total_rows']:,}")
    logger.info(f"Zones loaded: {stats['zones_loaded']}")
    logger.info("=" * 60)
    
    return stats


# Entry point for direct execution
if __name__ == "__main__":
    import argparse
    
    from src.utils.spark_session import create_spark_session
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Bronze Ingestion Job")
    parser.add_argument(
        "--months",
        nargs="+",
        required=True,
        help="Months to process (format: YYYY-MM)"
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Skip downloading (use existing files)"
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Force re-download even if files exist"
    )
    
    args = parser.parse_args()
    
    # Load config and create Spark session
    config = PipelineConfig.load()
    spark = create_spark_session(config, app_name="BronzeIngestion")
    
    try:
        # Run job
        stats = run_bronze_ingestion(
            config=config,
            spark=spark,
            months=args.months,
            download=not args.no_download,
            force_download=args.force_download,
        )
        print(f"\nJob completed successfully: {stats}")
    finally:
        spark.stop()
