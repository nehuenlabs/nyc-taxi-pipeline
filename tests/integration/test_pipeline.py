"""
Integration Tests for NYC Taxi Pipeline
========================================

These tests verify the end-to-end functionality of pipeline components
working together, including data transformations, schema validation,
and database interactions.

Requirements:
    - PySpark must be available
    - Tests use in-memory data (no external dependencies)
    
Run with:
    pytest tests/integration/ -v
"""

import os
import sys
import tempfile
import shutil
from datetime import datetime, date
from decimal import Decimal

import pytest

# Skip all tests if PySpark is not available
pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("NYC Taxi Pipeline Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="module")
def temp_dir():
    """Create a temporary directory for test outputs."""
    temp_path = tempfile.mkdtemp(prefix="nyc_taxi_test_")
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_trip_data(spark):
    """Create sample taxi trip data."""
    schema = StructType([
        StructField("VendorID", LongType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("RatecodeID", LongType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True),
    ])
    
    data = [
        # Valid records
        (1, datetime(2023, 1, 15, 10, 30), datetime(2023, 1, 15, 10, 45),
         2, 3.5, 161, 237, 1, "N", 1, 15.50, 1.0, 0.5, 0.3, 3.0, 0.0, 20.30, 2.5, 0.0),
        (2, datetime(2023, 1, 15, 14, 0), datetime(2023, 1, 15, 14, 30),
         1, 8.2, 138, 265, 1, "N", 2, 28.00, 0.0, 0.5, 0.3, 0.0, 6.55, 35.35, 2.5, 0.0),
        (1, datetime(2023, 1, 16, 8, 15), datetime(2023, 1, 16, 8, 45),
         3, 5.1, 237, 161, 1, "N", 1, 22.00, 2.5, 0.5, 0.3, 5.0, 0.0, 30.30, 2.5, 0.0),
        # Edge case: zero distance
        (1, datetime(2023, 1, 16, 9, 0), datetime(2023, 1, 16, 9, 5),
         1, 0.0, 161, 161, 1, "N", 1, 5.00, 0.0, 0.5, 0.3, 1.0, 0.0, 6.80, 2.5, 0.0),
        # Edge case: high fare
        (2, datetime(2023, 1, 17, 6, 0), datetime(2023, 1, 17, 7, 30),
         2, 45.0, 132, 265, 2, "N", 1, 150.00, 0.0, 0.5, 0.3, 30.0, 12.0, 195.30, 0.0, 1.75),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_zone_data(spark):
    """Create sample zone lookup data."""
    from src.models.schemas import ZONE_LOOKUP_SCHEMA
    
    data = [
        (1, "EWR", "Newark Airport", "EWR"),
        (132, "Queens", "JFK Airport", "Airports"),
        (138, "Queens", "LaGuardia Airport", "Airports"),
        (161, "Manhattan", "Midtown Center", "Yellow Zone"),
        (237, "Manhattan", "Upper East Side South", "Yellow Zone"),
        (265, "Queens", "Astoria", "Boro Zone"),
    ]
    
    return spark.createDataFrame(data, ZONE_LOOKUP_SCHEMA)


# =============================================================================
# Schema Validation Tests
# =============================================================================

class TestSchemaValidation:
    """Test schema enforcement and validation."""
    
    def test_trip_data_schema_fields(self, sample_trip_data):
        """Verify trip data has all required fields."""
        required_fields = [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
            "payment_type", "fare_amount", "total_amount"
        ]
        
        actual_fields = sample_trip_data.columns
        
        for field in required_fields:
            assert field in actual_fields, f"Missing required field: {field}"
    
    def test_zone_data_schema_fields(self, sample_zone_data):
        """Verify zone lookup has required fields."""
        required_fields = ["LocationID", "Borough", "Zone", "service_zone"]
        
        actual_fields = sample_zone_data.columns
        
        for field in required_fields:
            assert field in actual_fields, f"Missing zone field: {field}"
    
    def test_datetime_types(self, sample_trip_data):
        """Verify datetime columns have correct types."""
        schema_dict = {f.name: f.dataType for f in sample_trip_data.schema.fields}
        
        assert isinstance(schema_dict["tpep_pickup_datetime"], TimestampType)
        assert isinstance(schema_dict["tpep_dropoff_datetime"], TimestampType)


# =============================================================================
# Data Transformation Tests
# =============================================================================

class TestDataTransformations:
    """Test data transformation logic."""
    
    def test_add_metadata_columns(self, spark, sample_trip_data):
        """Test adding metadata columns to raw data."""
        source_file = "yellow_tripdata_2023-01.parquet"
        ingestion_ts = datetime.utcnow()
        
        df_with_meta = (
            sample_trip_data
            .withColumn("_source_file", F.lit(source_file))
            .withColumn("_ingestion_ts", F.lit(ingestion_ts))
            .withColumn("_year", F.year("tpep_pickup_datetime"))
            .withColumn("_month", F.month("tpep_pickup_datetime"))
        )
        
        assert "_source_file" in df_with_meta.columns
        assert "_ingestion_ts" in df_with_meta.columns
        assert "_year" in df_with_meta.columns
        assert "_month" in df_with_meta.columns
        
        # Verify values
        row = df_with_meta.first()
        assert row["_source_file"] == source_file
        assert row["_year"] == 2023
        assert row["_month"] == 1
    
    def test_join_zone_lookup(self, spark, sample_trip_data, sample_zone_data):
        """Test joining trip data with zone lookup."""
        df_joined = (
            sample_trip_data
            .join(
                sample_zone_data.alias("pu"),
                sample_trip_data["PULocationID"] == F.col("pu.LocationID"),
                "left"
            )
            .select(
                sample_trip_data["*"],
                F.col("pu.Borough").alias("pickup_borough"),
                F.col("pu.Zone").alias("pickup_zone"),
            )
        )
        
        assert "pickup_borough" in df_joined.columns
        assert "pickup_zone" in df_joined.columns
        
        # Verify a specific join
        midtown_trips = df_joined.filter(F.col("pickup_zone") == "Midtown Center")
        assert midtown_trips.count() >= 1
    
    def test_calculate_trip_duration(self, sample_trip_data):
        """Test trip duration calculation."""
        df_with_duration = sample_trip_data.withColumn(
            "trip_duration_minutes",
            (F.col("tpep_dropoff_datetime").cast("long") - 
             F.col("tpep_pickup_datetime").cast("long")) / 60
        )
        
        assert "trip_duration_minutes" in df_with_duration.columns
        
        # First trip should be ~15 minutes
        first_trip = df_with_duration.first()
        assert abs(first_trip["trip_duration_minutes"] - 15.0) < 1.0
    
    def test_filter_invalid_records(self, spark, sample_trip_data):
        """Test filtering out invalid records."""
        # Add some invalid records
        invalid_data = [
            # Negative fare
            (1, datetime(2023, 1, 15, 10, 0), datetime(2023, 1, 15, 10, 15),
             1, 2.0, 161, 237, 1, "N", 1, -5.00, 0.0, 0.5, 0.3, 0.0, 0.0, -4.20, 0.0, 0.0),
            # Future date
            (1, datetime(2030, 1, 15, 10, 0), datetime(2030, 1, 15, 10, 15),
             1, 2.0, 161, 237, 1, "N", 1, 10.00, 0.0, 0.5, 0.3, 0.0, 0.0, 10.80, 0.0, 0.0),
        ]
        
        df_with_invalid = sample_trip_data.union(
            spark.createDataFrame(invalid_data, sample_trip_data.schema)
        )
        
        initial_count = df_with_invalid.count()
        
        # Apply filters
        df_valid = df_with_invalid.filter(
            (F.col("fare_amount") >= 0) &
            (F.col("tpep_pickup_datetime") < F.lit(datetime(2025, 1, 1)))
        )
        
        # Should have filtered out 2 invalid records
        assert df_valid.count() == initial_count - 2
    
    def test_date_dimension_extraction(self, sample_trip_data):
        """Test extracting date dimension attributes."""
        df_with_date_attrs = sample_trip_data.select(
            F.col("tpep_pickup_datetime").alias("pickup_datetime"),
            F.year("tpep_pickup_datetime").alias("year"),
            F.month("tpep_pickup_datetime").alias("month"),
            F.dayofmonth("tpep_pickup_datetime").alias("day"),
            F.dayofweek("tpep_pickup_datetime").alias("day_of_week"),
            F.weekofyear("tpep_pickup_datetime").alias("week_of_year"),
            F.quarter("tpep_pickup_datetime").alias("quarter"),
        )
        
        row = df_with_date_attrs.first()
        
        assert row["year"] == 2023
        assert row["month"] == 1
        assert row["day"] == 15
        assert row["quarter"] == 1
    
    def test_time_dimension_extraction(self, sample_trip_data):
        """Test extracting time dimension attributes."""
        df_with_time_attrs = sample_trip_data.select(
            F.col("tpep_pickup_datetime"),
            F.hour("tpep_pickup_datetime").alias("hour"),
            F.minute("tpep_pickup_datetime").alias("minute"),
            F.when(F.hour("tpep_pickup_datetime") < 12, "AM")
             .otherwise("PM").alias("period"),
            F.when(
                (F.hour("tpep_pickup_datetime").between(7, 9)) |
                (F.hour("tpep_pickup_datetime").between(16, 19)),
                True
            ).otherwise(False).alias("is_rush_hour"),
        )
        
        # Verify first record (13:30 PM)
        row = df_with_time_attrs.first()
        assert row["hour"] == 13
        assert row["minute"] == 30
        assert row["period"] == "PM"


# =============================================================================
# Data Quality Tests
# =============================================================================

class TestDataQuality:
    """Test data quality checks."""
    
    def test_no_null_required_fields(self, sample_trip_data):
        """Verify no nulls in required fields."""
        required_fields = ["VendorID", "tpep_pickup_datetime", "fare_amount"]
        
        for field in required_fields:
            null_count = sample_trip_data.filter(F.col(field).isNull()).count()
            assert null_count == 0, f"Found {null_count} nulls in {field}"
    
    def test_fare_amount_range(self, sample_trip_data):
        """Verify fare amounts are within reasonable range."""
        out_of_range = sample_trip_data.filter(
            (F.col("fare_amount") < 0) | (F.col("fare_amount") > 500)
        ).count()
        
        assert out_of_range == 0, f"Found {out_of_range} fares out of range"
    
    def test_trip_distance_non_negative(self, sample_trip_data):
        """Verify trip distances are non-negative."""
        negative_distance = sample_trip_data.filter(
            F.col("trip_distance") < 0
        ).count()
        
        assert negative_distance == 0
    
    def test_valid_vendor_ids(self, sample_trip_data):
        """Verify vendor IDs are valid (1 or 2)."""
        invalid_vendors = sample_trip_data.filter(
            ~F.col("VendorID").isin([1, 2])
        ).count()
        
        assert invalid_vendors == 0
    
    def test_pickup_before_dropoff(self, sample_trip_data):
        """Verify pickup time is before dropoff time."""
        invalid_times = sample_trip_data.filter(
            F.col("tpep_pickup_datetime") > F.col("tpep_dropoff_datetime")
        ).count()
        
        assert invalid_times == 0


# =============================================================================
# Aggregation Tests
# =============================================================================

class TestAggregations:
    """Test aggregation and summarization logic."""
    
    def test_trips_per_vendor(self, sample_trip_data):
        """Test aggregating trips by vendor."""
        vendor_counts = (
            sample_trip_data
            .groupBy("VendorID")
            .agg(F.count("*").alias("trip_count"))
            .collect()
        )
        
        vendor_dict = {row["VendorID"]: row["trip_count"] for row in vendor_counts}
        
        assert 1 in vendor_dict
        assert 2 in vendor_dict
        assert sum(vendor_dict.values()) == sample_trip_data.count()
    
    def test_revenue_by_payment_type(self, sample_trip_data):
        """Test revenue aggregation by payment type."""
        revenue = (
            sample_trip_data
            .groupBy("payment_type")
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("avg_revenue"),
                F.count("*").alias("trip_count"),
            )
            .collect()
        )
        
        assert len(revenue) > 0
        
        for row in revenue:
            assert row["total_revenue"] > 0
            assert row["avg_revenue"] > 0
    
    def test_trips_by_hour(self, sample_trip_data):
        """Test trip distribution by hour."""
        hourly_trips = (
            sample_trip_data
            .withColumn("hour", F.hour("tpep_pickup_datetime"))
            .groupBy("hour")
            .agg(F.count("*").alias("trip_count"))
            .orderBy("hour")
            .collect()
        )
        
        assert len(hourly_trips) > 0
        
        total_trips = sum(row["trip_count"] for row in hourly_trips)
        assert total_trips == sample_trip_data.count()


# =============================================================================
# Write/Read Tests
# =============================================================================

class TestWriteRead:
    """Test writing and reading data."""
    
    def test_write_read_parquet(self, spark, sample_trip_data, temp_dir):
        """Test writing and reading Parquet files."""
        output_path = os.path.join(temp_dir, "trips_parquet")
        
        # Write
        sample_trip_data.write.mode("overwrite").parquet(output_path)
        
        # Read back
        df_read = spark.read.parquet(output_path)
        
        assert df_read.count() == sample_trip_data.count()
        assert set(df_read.columns) == set(sample_trip_data.columns)
    
    def test_write_partitioned(self, spark, sample_trip_data, temp_dir):
        """Test writing partitioned data."""
        output_path = os.path.join(temp_dir, "trips_partitioned")
        
        df_with_partition = sample_trip_data.withColumn(
            "pickup_date", F.to_date("tpep_pickup_datetime")
        )
        
        # Write partitioned by date
        (
            df_with_partition
            .write
            .mode("overwrite")
            .partitionBy("pickup_date")
            .parquet(output_path)
        )
        
        # Read back
        df_read = spark.read.parquet(output_path)
        
        assert df_read.count() == sample_trip_data.count()
        assert "pickup_date" in df_read.columns
    
    def test_overwrite_partition(self, spark, sample_trip_data, temp_dir):
        """Test partition overwrite (idempotency)."""
        output_path = os.path.join(temp_dir, "trips_idempotent")
        
        # Initial write
        sample_trip_data.write.mode("overwrite").parquet(output_path)
        initial_count = spark.read.parquet(output_path).count()
        
        # Write again (should overwrite, not append)
        sample_trip_data.write.mode("overwrite").parquet(output_path)
        final_count = spark.read.parquet(output_path).count()
        
        assert initial_count == final_count, "Overwrite should not change count"


# =============================================================================
# Monitoring Integration Tests
# =============================================================================

class TestMonitoringIntegration:
    """Test monitoring module integration."""
    
    def test_pipeline_monitor_basic(self):
        """Test basic pipeline monitoring."""
        from src.monitoring import PipelineMonitor
        
        monitor = PipelineMonitor("test_pipeline", json_logging=False)
        
        with monitor.track_stage("stage_1") as stage:
            stage.records_in = 100
            stage.records_out = 95
            stage.records_rejected = 5
        
        summary = monitor.finish()
        
        assert summary["pipeline"] == "test_pipeline"
        assert summary["stages_total"] == 1
        assert summary["total_records_in"] == 100
        assert summary["total_records_out"] == 95
    
    def test_pipeline_monitor_multiple_stages(self):
        """Test monitoring multiple stages."""
        from src.monitoring import PipelineMonitor
        
        monitor = PipelineMonitor("multi_stage_pipeline", json_logging=False)
        
        with monitor.track_stage("extract") as stage:
            stage.records_out = 1000
        
        with monitor.track_stage("transform") as stage:
            stage.records_in = 1000
            stage.records_out = 950
            stage.records_rejected = 50
        
        with monitor.track_stage("load") as stage:
            stage.records_in = 950
            stage.records_out = 950
        
        summary = monitor.finish()
        
        assert summary["stages_total"] == 3
        assert len([s for s in summary["stages"] if s["success"]]) == 3
    
    def test_alert_on_high_rejection(self):
        """Test that alerts are triggered on high rejection rates."""
        from src.monitoring import PipelineMonitor
        
        monitor = PipelineMonitor("alert_test", json_logging=False)
        
        with monitor.track_stage("bad_stage") as stage:
            stage.records_in = 100
            stage.records_out = 70
            stage.records_rejected = 30  # 30% rejection rate
        
        summary = monitor.finish()
        
        # Should have triggered a critical alert (>20% rejection)
        assert summary["alerts_count"] > 0
        assert summary["alerts_by_level"]["critical"] > 0
