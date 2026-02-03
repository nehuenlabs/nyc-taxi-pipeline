"""
Dimensional Model Definitions
=============================

Defines the dimensional model for NYC Taxi data warehouse.
Includes reference data for dimensions and transformation logic.

Star Schema:
    - fact_trips: One row per taxi trip
    - dim_date: Calendar dimension
    - dim_time: Time of day dimension
    - dim_location: Taxi zones
    - dim_vendor: Taxi vendors
    - dim_ratecode: Rate codes
    - dim_payment_type: Payment methods
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================
# REFERENCE DATA
# ============================================

VENDORS = {
    1: {"name": "Creative Mobile Technologies, LLC"},
    2: {"name": "VeriFone Inc."},
}

RATE_CODES = {
    1: {"name": "Standard rate", "description": "Standard metered fare"},
    2: {"name": "JFK", "description": "JFK Airport flat fare"},
    3: {"name": "Newark", "description": "Newark Airport negotiated fare"},
    4: {"name": "Nassau/Westchester", "description": "Nassau or Westchester fare"},
    5: {"name": "Negotiated fare", "description": "Negotiated flat fare"},
    6: {"name": "Group ride", "description": "Group ride fare"},
    99: {"name": "Unknown", "description": "Unknown or invalid rate code"},
}

PAYMENT_TYPES = {
    0: {"name": "Unknown/Missing"},  # For NULL or invalid payment types in source data
    1: {"name": "Credit card"},
    2: {"name": "Cash"},
    3: {"name": "No charge"},
    4: {"name": "Dispute"},
    5: {"name": "Unknown"},
    6: {"name": "Voided trip"},
}

TIME_OF_DAY_RANGES = [
    (0, 6, "Night"),
    (6, 12, "Morning"),
    (12, 17, "Afternoon"),
    (17, 21, "Evening"),
    (21, 24, "Night"),
]

# Rush hour definitions (weekdays only)
RUSH_HOURS = [
    (7, 10),   # Morning rush
    (16, 19),  # Evening rush
]


# ============================================
# DIMENSION GENERATORS
# ============================================

def generate_dim_date(
    spark: SparkSession,
    start_date: date,
    end_date: date,
) -> DataFrame:
    """
    Generate date dimension table.
    
    Args:
        spark: SparkSession
        start_date: First date to include
        end_date: Last date to include
    
    Returns:
        DataFrame with date dimension
    """
    # Generate date range
    dates = []
    current = start_date
    while current <= end_date:
        dates.append((current,))
        current += timedelta(days=1)
    
    # Create DataFrame
    df = spark.createDataFrame(dates, ["full_date"])
    
    # Add derived columns
    df = (
        df
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date").cast("short"))
        .withColumn("month", F.month("full_date").cast("short"))
        .withColumn("day", F.dayofmonth("full_date").cast("short"))
        .withColumn("day_of_week", F.dayofweek("full_date").cast("short"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date").cast("short"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
        .withColumn("is_holiday", F.lit(False))  # Can be enhanced with holiday calendar
        .withColumn("quarter", F.quarter("full_date").cast("short"))
    )
    
    # Select and order columns
    return df.select(
        "date_key",
        "full_date",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "week_of_year",
        "is_weekend",
        "is_holiday",
        "quarter",
    )


def generate_dim_time(spark: SparkSession) -> DataFrame:
    """
    Generate time dimension table (one row per minute).
    
    Args:
        spark: SparkSession
    
    Returns:
        DataFrame with time dimension
    """
    # Generate all minutes of the day
    times = []
    for hour in range(24):
        for minute in range(60):
            times.append((hour, minute))
    
    # Create DataFrame
    df = spark.createDataFrame(times, ["hour", "minute"])
    
    # Determine time of day
    time_of_day_expr = F.when(F.col("hour") < 6, "Night")
    for start, end, name in TIME_OF_DAY_RANGES:
        if name != "Night":  # Already handled
            time_of_day_expr = time_of_day_expr.when(
                (F.col("hour") >= start) & (F.col("hour") < end),
                name
            )
    time_of_day_expr = time_of_day_expr.otherwise("Night")
    
    # Determine rush hour
    rush_hour_expr = F.lit(False)
    for start, end in RUSH_HOURS:
        rush_hour_expr = rush_hour_expr | (
            (F.col("hour") >= start) & (F.col("hour") < end)
        )
    
    # Add derived columns
    df = (
        df
        .withColumn("time_key", (F.col("hour") * 100 + F.col("minute")).cast("int"))
        .withColumn("hour", F.col("hour").cast("short"))
        .withColumn("minute", F.col("minute").cast("short"))
        .withColumn("time_of_day", time_of_day_expr)
        .withColumn("is_rush_hour", rush_hour_expr)
        .withColumn("is_night", (F.col("hour") < 6) | (F.col("hour") >= 21))
    )
    
    return df.select(
        "time_key",
        "hour",
        "minute",
        "time_of_day",
        "is_rush_hour",
        "is_night",
    )


def generate_dim_vendor(spark: SparkSession) -> DataFrame:
    """
    Generate vendor dimension from reference data.
    
    Args:
        spark: SparkSession
    
    Returns:
        DataFrame with vendor dimension
    """
    data = [
        (vendor_id, vendor_id, info["name"])
        for vendor_id, info in VENDORS.items()
    ]
    
    return spark.createDataFrame(
        data,
        ["vendor_key", "vendor_id", "vendor_name"]
    )


def generate_dim_ratecode(spark: SparkSession) -> DataFrame:
    """
    Generate rate code dimension from reference data.
    
    Args:
        spark: SparkSession
    
    Returns:
        DataFrame with rate code dimension
    """
    data = [
        (ratecode_id, ratecode_id, info["name"], info["description"])
        for ratecode_id, info in RATE_CODES.items()
    ]
    
    return spark.createDataFrame(
        data,
        ["ratecode_key", "ratecode_id", "ratecode_name", "ratecode_description"]
    )


def generate_dim_payment_type(spark: SparkSession) -> DataFrame:
    """
    Generate payment type dimension from reference data.
    
    Args:
        spark: SparkSession
    
    Returns:
        DataFrame with payment type dimension
    """
    data = [
        (payment_id, payment_id, info["name"])
        for payment_id, info in PAYMENT_TYPES.items()
    ]
    
    return spark.createDataFrame(
        data,
        ["payment_type_key", "payment_type_id", "payment_type_name"]
    )


def generate_dim_location(
    spark: SparkSession,
    zones_df: DataFrame,
) -> DataFrame:
    """
    Generate location dimension from zone lookup data.
    
    Args:
        spark: SparkSession
        zones_df: DataFrame with zone lookup data
    
    Returns:
        DataFrame with location dimension
    """
    return (
        zones_df
        .withColumn("location_key", F.col("LocationID"))
        .withColumn("location_id", F.col("LocationID"))
        .withColumn("borough", F.col("Borough"))
        .withColumn("zone_name", F.col("Zone"))
        .select(
            "location_key",
            "location_id",
            "borough",
            "zone_name",
            "service_zone",
        )
    )


# ============================================
# FACT TABLE TRANSFORMATION
# ============================================

def transform_to_fact_trips(
    spark: SparkSession,
    trips_df: DataFrame,
    dim_location: DataFrame,
) -> DataFrame:
    """
    Transform bronze trips data to fact_trips.
    
    Args:
        spark: SparkSession
        trips_df: Bronze layer trips DataFrame
        dim_location: Location dimension DataFrame
    
    Returns:
        DataFrame with fact_trips schema
    """
    # Generate surrogate key using monotonically_increasing_id
    # In production, you might want a more sophisticated approach
    df = trips_df.withColumn(
        "trip_key",
        F.monotonically_increasing_id()
    )
    
    # Generate date key from pickup datetime
    df = df.withColumn(
        "date_key",
        F.date_format("tpep_pickup_datetime", "yyyyMMdd").cast("int")
    )
    
    # Generate time keys
    df = df.withColumn(
        "pickup_time_key",
        (F.hour("tpep_pickup_datetime") * 100 + F.minute("tpep_pickup_datetime")).cast("int")
    )
    df = df.withColumn(
        "dropoff_time_key",
        (F.hour("tpep_dropoff_datetime") * 100 + F.minute("tpep_dropoff_datetime")).cast("int")
    )
    
    # Location keys (already match LocationID)
    df = df.withColumn("pickup_location_key", F.col("PULocationID"))
    df = df.withColumn("dropoff_location_key", F.col("DOLocationID"))
    
    # Vendor key
    df = df.withColumn("vendor_key", F.coalesce(F.col("VendorID"), F.lit(1)))
    
    # Ratecode key
    df = df.withColumn(
        "ratecode_key",
        F.coalesce(F.col("RatecodeID").cast("int"), F.lit(1))
    )
    
    # Payment type key
    df = df.withColumn(
        "payment_type_key",
        F.coalesce(F.col("payment_type"), F.lit(5))  # 5 = Unknown
    )
    
    # Calculate trip duration in minutes
    df = df.withColumn(
        "trip_duration_minutes",
        (
            F.unix_timestamp("tpep_dropoff_datetime") -
            F.unix_timestamp("tpep_pickup_datetime")
        ) / 60.0
    )
    
    # Clean and cast measures
    df = (
        df
        .withColumn("passenger_count", F.col("passenger_count").cast("short"))
        .withColumn("trip_distance_miles", F.col("trip_distance").cast("decimal(10,2)"))
        .withColumn("trip_duration_minutes", F.col("trip_duration_minutes").cast("decimal(10,2)"))
        .withColumn("fare_amount", F.col("fare_amount").cast("decimal(10,2)"))
        .withColumn("extra_amount", F.col("extra").cast("decimal(10,2)"))
        .withColumn("mta_tax", F.col("mta_tax").cast("decimal(10,2)"))
        .withColumn("tip_amount", F.col("tip_amount").cast("decimal(10,2)"))
        .withColumn("tolls_amount", F.col("tolls_amount").cast("decimal(10,2)"))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast("decimal(10,2)"))
        .withColumn(
            "congestion_surcharge",
            F.coalesce(F.col("congestion_surcharge"), F.lit(0)).cast("decimal(10,2)")
        )
        .withColumn(
            "airport_fee",
            F.coalesce(F.col("Airport_fee"), F.lit(0)).cast("decimal(10,2)")
        )
        .withColumn("total_amount", F.col("total_amount").cast("decimal(10,2)"))
    )
    
    # Select final columns
    return df.select(
        "trip_key",
        "date_key",
        "pickup_time_key",
        "dropoff_time_key",
        "pickup_location_key",
        "dropoff_location_key",
        "vendor_key",
        "ratecode_key",
        "payment_type_key",
        "passenger_count",
        "trip_distance_miles",
        "trip_duration_minutes",
        "fare_amount",
        "extra_amount",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "total_amount",
        "_source_file",
        "_ingestion_ts",
    )


# ============================================
# DATA QUALITY FILTERS
# ============================================

def apply_quality_filters(df: DataFrame) -> DataFrame:
    """
    Apply data quality filters to trips data.
    
    Removes records with:
        - Invalid pickup/dropoff times
        - Negative amounts
        - Zero or negative distance (for non-zero fares)
        - Unreasonable trip durations
    
    Args:
        df: Raw trips DataFrame
    
    Returns:
        Filtered DataFrame
    """
    return (
        df
        # Valid timestamps
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        
        # Valid amounts
        .filter(F.col("fare_amount") >= 0)
        .filter(F.col("total_amount") >= 0)
        .filter(F.col("trip_distance") >= 0)
        
        # Reasonable trip duration (< 24 hours)
        .filter(
            (F.unix_timestamp("tpep_dropoff_datetime") - 
             F.unix_timestamp("tpep_pickup_datetime")) < 86400
        )
        
        # Valid location IDs
        .filter(F.col("PULocationID").isNotNull())
        .filter(F.col("DOLocationID").isNotNull())
        .filter(F.col("PULocationID") > 0)
        .filter(F.col("DOLocationID") > 0)
    )


# ============================================
# MODEL METADATA
# ============================================

@dataclass
class DimensionalModel:
    """Metadata about the dimensional model."""
    
    fact_table: str = "fact_trips"
    
    dimensions: List[str] = None
    
    def __post_init__(self):
        if self.dimensions is None:
            self.dimensions = [
                "dim_date",
                "dim_time",
                "dim_location",
                "dim_vendor",
                "dim_ratecode",
                "dim_payment_type",
            ]
    
    @property
    def all_tables(self) -> List[str]:
        """Get all table names in the model."""
        return [self.fact_table] + self.dimensions


# Singleton instance
MODEL = DimensionalModel()
