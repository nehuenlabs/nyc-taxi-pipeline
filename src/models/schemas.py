"""
PySpark Schema Definitions
==========================

Defines StructType schemas for NYC Taxi data.

Schemas:
    - YELLOW_TAXI_SCHEMA: Raw yellow taxi trip data
    - ZONE_LOOKUP_SCHEMA: Taxi zone lookup table
    - BRONZE_TRIP_SCHEMA: Bronze layer with metadata
    - Various dimension and fact schemas
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ============================================
# RAW DATA SCHEMAS
# ============================================

YELLOW_TAXI_SCHEMA = StructType([
    # Vendor - INT64 in source file
    StructField("VendorID", LongType(), True),
    
    # Timestamps
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    
    # Trip info
    StructField("passenger_count", LongType(), True),  # INT64 in source
    StructField("trip_distance", DoubleType(), True),
    
    # Locations - INT64 in source file
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    
    # Rate and payment - INT64 in source file
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("payment_type", LongType(), True),
    
    # Fare components
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    
    # Additional fees (may not exist in older data)
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
])


ZONE_LOOKUP_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), False),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])


# ============================================
# BRONZE LAYER SCHEMAS
# ============================================

BRONZE_TRIP_SCHEMA = StructType([
    # Original fields
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    
    # Metadata fields
    StructField("_source_file", StringType(), False),
    StructField("_source_month", StringType(), False),
    StructField("_ingestion_ts", TimestampType(), False),
    
    # Partition columns
    StructField("_year", IntegerType(), False),
    StructField("_month", IntegerType(), False),
])


# ============================================
# DIMENSION SCHEMAS
# ============================================

DIM_DATE_SCHEMA = StructType([
    StructField("date_key", IntegerType(), False),  # YYYYMMDD
    StructField("full_date", DateType(), False),
    StructField("year", ShortType(), False),
    StructField("month", ShortType(), False),
    StructField("day", ShortType(), False),
    StructField("day_of_week", ShortType(), False),  # 1=Monday, 7=Sunday
    StructField("day_name", StringType(), False),
    StructField("week_of_year", ShortType(), False),
    StructField("is_weekend", BooleanType(), False),
    StructField("is_holiday", BooleanType(), False),
    StructField("quarter", ShortType(), False),
])


DIM_TIME_SCHEMA = StructType([
    StructField("time_key", IntegerType(), False),  # HHMM
    StructField("hour", ShortType(), False),
    StructField("minute", ShortType(), False),
    StructField("time_of_day", StringType(), False),  # Morning, Afternoon, Evening, Night
    StructField("is_rush_hour", BooleanType(), False),
    StructField("is_night", BooleanType(), False),
])


DIM_LOCATION_SCHEMA = StructType([
    StructField("location_key", IntegerType(), False),
    StructField("location_id", IntegerType(), False),
    StructField("borough", StringType(), True),
    StructField("zone_name", StringType(), True),
    StructField("service_zone", StringType(), True),
])


DIM_VENDOR_SCHEMA = StructType([
    StructField("vendor_key", IntegerType(), False),
    StructField("vendor_id", IntegerType(), False),
    StructField("vendor_name", StringType(), False),
])


DIM_RATECODE_SCHEMA = StructType([
    StructField("ratecode_key", IntegerType(), False),
    StructField("ratecode_id", IntegerType(), False),
    StructField("ratecode_name", StringType(), False),
    StructField("ratecode_description", StringType(), True),
])


DIM_PAYMENT_TYPE_SCHEMA = StructType([
    StructField("payment_type_key", IntegerType(), False),
    StructField("payment_type_id", IntegerType(), False),
    StructField("payment_type_name", StringType(), False),
])


# ============================================
# FACT SCHEMA
# ============================================

FACT_TRIPS_SCHEMA = StructType([
    # Surrogate key
    StructField("trip_key", LongType(), False),
    
    # Foreign keys
    StructField("date_key", IntegerType(), False),
    StructField("pickup_time_key", IntegerType(), False),
    StructField("dropoff_time_key", IntegerType(), False),
    StructField("pickup_location_key", IntegerType(), False),
    StructField("dropoff_location_key", IntegerType(), False),
    StructField("vendor_key", IntegerType(), False),
    StructField("ratecode_key", IntegerType(), False),
    StructField("payment_type_key", IntegerType(), False),
    
    # Measures
    StructField("passenger_count", ShortType(), True),
    StructField("trip_distance_miles", DecimalType(10, 2), True),
    StructField("trip_duration_minutes", DecimalType(10, 2), True),
    StructField("fare_amount", DecimalType(10, 2), True),
    StructField("extra_amount", DecimalType(10, 2), True),
    StructField("mta_tax", DecimalType(10, 2), True),
    StructField("tip_amount", DecimalType(10, 2), True),
    StructField("tolls_amount", DecimalType(10, 2), True),
    StructField("improvement_surcharge", DecimalType(10, 2), True),
    StructField("congestion_surcharge", DecimalType(10, 2), True),
    StructField("airport_fee", DecimalType(10, 2), True),
    StructField("total_amount", DecimalType(10, 2), True),
    
    # Audit columns
    StructField("_source_file", StringType(), False),
    StructField("_ingestion_ts", TimestampType(), False),
])


# ============================================
# SCHEMA UTILITIES
# ============================================

def get_schema_field_names(schema: StructType) -> list[str]:
    """
    Get list of field names from a schema.
    
    Args:
        schema: PySpark StructType schema
    
    Returns:
        List of field names
    """
    return [field.name for field in schema.fields]


def schema_to_ddl(schema: StructType, table_name: str) -> str:
    """
    Convert PySpark schema to PostgreSQL DDL.
    
    Args:
        schema: PySpark StructType schema
        table_name: Table name for DDL
    
    Returns:
        CREATE TABLE statement
    """
    type_mapping = {
        "IntegerType": "INTEGER",
        "LongType": "BIGINT",
        "ShortType": "SMALLINT",
        "DoubleType": "DOUBLE PRECISION",
        "StringType": "VARCHAR(255)",
        "BooleanType": "BOOLEAN",
        "DateType": "DATE",
        "TimestampType": "TIMESTAMP",
        "DecimalType(10,2)": "DECIMAL(10,2)",
    }
    
    columns = []
    for field in schema.fields:
        spark_type = str(field.dataType)
        pg_type = type_mapping.get(spark_type, "TEXT")
        nullable = "" if field.nullable else " NOT NULL"
        columns.append(f"    {field.name} {pg_type}{nullable}")
    
    return f"CREATE TABLE {table_name} (\n" + ",\n".join(columns) + "\n);"
