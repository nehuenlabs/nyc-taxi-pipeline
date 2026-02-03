"""
Dimensional Transform Job
=========================

Transforms Bronze layer data into the dimensional model (Gold layer).

This job:
    1. Reads trips from Bronze layer
    2. Applies data quality filters
    3. Loads dimension tables (dim_location from zones)
    4. Transforms trips to fact_trips
    5. Writes to PostgreSQL

Usage:
    python -m src.jobs.dimensional_transform --months 2023-01 2023-02
"""

from __future__ import annotations

import logging
from datetime import date
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.models.dimensional import (
    apply_quality_filters,
    generate_dim_date,
    generate_dim_location,
    generate_dim_payment_type,
    generate_dim_ratecode,
    generate_dim_time,
    generate_dim_vendor,
    transform_to_fact_trips,
)
from src.utils.config import PipelineConfig
from src.utils.database import DatabaseWriter, execute_sql, table_exists

logger = logging.getLogger(__name__)


def read_bronze_trips(
    spark: SparkSession,
    bronze_path: str,
    months: Optional[List[str]] = None,
) -> DataFrame:
    """
    Read trips from Bronze layer.
    
    Args:
        spark: SparkSession
        bronze_path: Path to bronze trips
        months: Optional list of months to filter (format: "YYYY-MM")
    
    Returns:
        DataFrame with bronze trips
    """
    logger.info(f"Reading bronze trips from: {bronze_path}")
    
    df = spark.read.parquet(bronze_path)
    
    # Filter by months if specified
    if months:
        year_months = []
        for m in months:
            year, month = m.split("-")
            year_months.append((int(year), int(month)))
        
        # Build filter condition
        filter_cond = None
        for year, month in year_months:
            cond = (F.col("_year") == year) & (F.col("_month") == month)
            filter_cond = cond if filter_cond is None else (filter_cond | cond)
        
        df = df.filter(filter_cond)
        logger.info(f"Filtered to months: {months}")
    
    row_count = df.count()
    logger.info(f"Read {row_count:,} bronze trips")
    
    return df


def read_bronze_zones(
    spark: SparkSession,
    bronze_path: str,
) -> DataFrame:
    """
    Read zone lookup from Bronze layer.
    
    Args:
        spark: SparkSession
        bronze_path: Path to bronze zones
    
    Returns:
        DataFrame with zone lookup
    """
    logger.info(f"Reading bronze zones from: {bronze_path}")
    
    df = spark.read.parquet(bronze_path)
    
    row_count = df.count()
    logger.info(f"Read {row_count:,} zones")
    
    return df


def load_dimension_to_postgres(
    df: DataFrame,
    table_name: str,
    config: PipelineConfig,
) -> None:
    """
    Load a dimension table to PostgreSQL.
    
    Args:
        df: Dimension DataFrame
        table_name: Target table name
        config: Pipeline configuration
    """
    logger.info(f"Loading dimension: {table_name}")
    
    writer = DatabaseWriter(config)
    writer.write_to_postgres(df, table_name, mode="overwrite")
    
    row_count = df.count()
    logger.info(f"Loaded {row_count:,} rows to {table_name}")


def load_fact_to_postgres(
    df: DataFrame,
    table_name: str,
    config: PipelineConfig,
    mode: str = "append",
) -> None:
    """
    Load fact table to PostgreSQL.
    
    Args:
        df: Fact DataFrame
        table_name: Target table name
        config: Pipeline configuration
        mode: Write mode ("append" or "overwrite")
    """
    logger.info(f"Loading fact table: {table_name}")
    logger.info(f"Mode: {mode}")
    
    writer = DatabaseWriter(config)
    
    # For fact table, we don't include trip_key (let PostgreSQL generate it)
    columns_to_write = [c for c in df.columns if c != "trip_key"]
    df_to_write = df.select(columns_to_write)
    
    writer.write_to_postgres(df_to_write, table_name, mode=mode)
    
    row_count = df.count()
    logger.info(f"Loaded {row_count:,} rows to {table_name}")


def get_date_range_from_trips(df: DataFrame) -> tuple:
    """
    Get min and max dates from trips DataFrame.
    
    Args:
        df: Trips DataFrame with tpep_pickup_datetime
    
    Returns:
        Tuple of (min_date, max_date)
    """
    date_stats = (
        df
        .select(
            F.min(F.to_date("tpep_pickup_datetime")).alias("min_date"),
            F.max(F.to_date("tpep_pickup_datetime")).alias("max_date")
        )
        .collect()[0]
    )
    
    return date_stats.min_date, date_stats.max_date


def run_dimensional_transform(
    config: PipelineConfig,
    spark: SparkSession,
    months: List[str],
    reload_dimensions: bool = True,
    fact_mode: str = "append",
) -> dict:
    """
    Run the dimensional transformation job.
    
    Args:
        config: Pipeline configuration
        spark: SparkSession
        months: List of months to process (format: "YYYY-MM")
        reload_dimensions: If True, reload all dimension tables
        fact_mode: Write mode for fact table ("append" or "overwrite")
    
    Returns:
        Dictionary with job statistics
    """
    logger.info("=" * 60)
    logger.info("DIMENSIONAL TRANSFORM JOB")
    logger.info("=" * 60)
    logger.info(f"Environment: {config.environment.value}")
    logger.info(f"Months to process: {months}")
    logger.info(f"Bronze path: {config.storage.bronze_path}")
    logger.info(f"Reload dimensions: {reload_dimensions}")
    logger.info(f"Fact mode: {fact_mode}")
    
    stats = {
        "months_processed": len(months),
        "trips_read": 0,
        "trips_after_filter": 0,
        "facts_written": 0,
        "dimensions_loaded": [],
    }
    
    # ========================================
    # Read Bronze Data
    # ========================================
    logger.info("-" * 40)
    logger.info("Reading Bronze Data")
    
    # Read trips
    trips_bronze_path = f"{config.storage.bronze_path}/trips"
    bronze_trips = read_bronze_trips(spark, trips_bronze_path, months)
    stats["trips_read"] = bronze_trips.count()
    
    # Read zones
    zones_bronze_path = f"{config.storage.bronze_path}/zones"
    bronze_zones = read_bronze_zones(spark, zones_bronze_path)
    
    # ========================================
    # Apply Quality Filters
    # ========================================
    logger.info("-" * 40)
    logger.info("Applying Quality Filters")
    
    filtered_trips = apply_quality_filters(bronze_trips)
    stats["trips_after_filter"] = filtered_trips.count()
    
    rejected = stats["trips_read"] - stats["trips_after_filter"]
    reject_pct = (rejected / stats["trips_read"] * 100) if stats["trips_read"] > 0 else 0
    logger.info(f"Filtered out {rejected:,} rows ({reject_pct:.2f}%)")
    
    # ========================================
    # Load Dimensions
    # ========================================
    if reload_dimensions:
        logger.info("-" * 40)
        logger.info("Loading Dimensions")
        
        # dim_vendor
        dim_vendor = generate_dim_vendor(spark)
        load_dimension_to_postgres(dim_vendor, "dim_vendor", config)
        stats["dimensions_loaded"].append("dim_vendor")
        
        # dim_ratecode
        dim_ratecode = generate_dim_ratecode(spark)
        load_dimension_to_postgres(dim_ratecode, "dim_ratecode", config)
        stats["dimensions_loaded"].append("dim_ratecode")
        
        # dim_payment_type
        dim_payment_type = generate_dim_payment_type(spark)
        load_dimension_to_postgres(dim_payment_type, "dim_payment_type", config)
        stats["dimensions_loaded"].append("dim_payment_type")
        
        # dim_time
        dim_time = generate_dim_time(spark)
        load_dimension_to_postgres(dim_time, "dim_time", config)
        stats["dimensions_loaded"].append("dim_time")
        
        # dim_location (from zones)
        dim_location = generate_dim_location(spark, bronze_zones)
        load_dimension_to_postgres(dim_location, "dim_location", config)
        stats["dimensions_loaded"].append("dim_location")
        
        # dim_date (based on trip date range, with buffer)
        min_date, max_date = get_date_range_from_trips(filtered_trips)
        # Extend range by 1 year on each side for safety
        start_date = date(min_date.year - 1, 1, 1)
        end_date = date(max_date.year + 1, 12, 31)
        
        dim_date = generate_dim_date(spark, start_date, end_date)
        load_dimension_to_postgres(dim_date, "dim_date", config)
        stats["dimensions_loaded"].append("dim_date")
        
        logger.info(f"Loaded {len(stats['dimensions_loaded'])} dimensions")
    
    # ========================================
    # Transform to Fact Table
    # ========================================
    logger.info("-" * 40)
    logger.info("Transforming to Fact Table")
    
    # Re-read dim_location for the join
    dim_location = generate_dim_location(spark, bronze_zones)
    
    # Transform to fact_trips
    fact_trips = transform_to_fact_trips(spark, filtered_trips, dim_location)
    
    # ========================================
    # Load Fact Table
    # ========================================
    logger.info("-" * 40)
    logger.info("Loading Fact Table")
    
    load_fact_to_postgres(fact_trips, "fact_trips", config, mode=fact_mode)
    stats["facts_written"] = fact_trips.count()
    
    # ========================================
    # Summary
    # ========================================
    logger.info("=" * 60)
    logger.info("DIMENSIONAL TRANSFORM COMPLETE")
    logger.info(f"Trips read: {stats['trips_read']:,}")
    logger.info(f"Trips after filter: {stats['trips_after_filter']:,}")
    logger.info(f"Facts written: {stats['facts_written']:,}")
    logger.info(f"Dimensions loaded: {stats['dimensions_loaded']}")
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
    parser = argparse.ArgumentParser(description="Dimensional Transform Job")
    parser.add_argument(
        "--months",
        nargs="+",
        required=True,
        help="Months to process (format: YYYY-MM)"
    )
    parser.add_argument(
        "--no-reload-dimensions",
        action="store_true",
        help="Skip reloading dimension tables"
    )
    parser.add_argument(
        "--fact-mode",
        choices=["append", "overwrite"],
        default="append",
        help="Write mode for fact table"
    )
    
    args = parser.parse_args()
    
    # Load config and create Spark session
    config = PipelineConfig.load()
    spark = create_spark_session(config, app_name="DimensionalTransform")
    
    try:
        # Run job
        stats = run_dimensional_transform(
            config=config,
            spark=spark,
            months=args.months,
            reload_dimensions=not args.no_reload_dimensions,
            fact_mode=args.fact_mode,
        )
        print(f"\nJob completed successfully: {stats}")
    finally:
        spark.stop()
