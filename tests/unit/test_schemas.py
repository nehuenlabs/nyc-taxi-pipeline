"""Unit tests for data schemas."""

import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
    TimestampType,
)


class TestYellowTaxiSchema:
    """Tests for Yellow Taxi schema definition."""

    def test_schema_has_required_fields(self):
        """Verify schema contains all required NYC TLC fields."""
        from src.models.schemas import YELLOW_TAXI_SCHEMA

        field_names = [field.name for field in YELLOW_TAXI_SCHEMA.fields]

        required_fields = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "total_amount",
        ]

        for field in required_fields:
            assert field in field_names, f"Missing required field: {field}"

    def test_schema_field_types(self):
        """Verify correct data types for key fields."""
        from src.models.schemas import YELLOW_TAXI_SCHEMA

        field_types = {field.name: field.dataType for field in YELLOW_TAXI_SCHEMA.fields}

        # Datetime fields should be TimestampType
        assert isinstance(field_types.get("tpep_pickup_datetime"), TimestampType)
        assert isinstance(field_types.get("tpep_dropoff_datetime"), TimestampType)

        # Numeric fields should be numeric types
        assert isinstance(field_types.get("trip_distance"), DoubleType)
        assert isinstance(field_types.get("fare_amount"), DoubleType)

    def test_schema_field_count(self):
        """Verify schema has expected number of fields."""
        from src.models.schemas import YELLOW_TAXI_SCHEMA
        
        # Yellow taxi schema should have around 19 fields
        assert len(YELLOW_TAXI_SCHEMA.fields) >= 15


class TestZoneLookupSchema:
    """Tests for Taxi Zone Lookup schema."""

    def test_zone_schema_fields(self):
        """Verify zone lookup schema has required fields."""
        from src.models.schemas import ZONE_LOOKUP_SCHEMA

        field_names = [field.name for field in ZONE_LOOKUP_SCHEMA.fields]

        required_fields = ["LocationID", "Borough", "Zone", "service_zone"]

        for field in required_fields:
            assert field in field_names, f"Missing zone field: {field}"

    def test_zone_schema_types(self):
        """Verify zone lookup has correct types."""
        from src.models.schemas import ZONE_LOOKUP_SCHEMA

        field_types = {field.name: field.dataType for field in ZONE_LOOKUP_SCHEMA.fields}
        
        assert isinstance(field_types.get("LocationID"), IntegerType)
        assert isinstance(field_types.get("Borough"), StringType)


class TestBronzeSchema:
    """Tests for Bronze layer schema."""

    def test_bronze_has_metadata_fields(self):
        """Verify bronze schema includes metadata columns."""
        from src.models.schemas import BRONZE_TRIP_SCHEMA

        field_names = [field.name for field in BRONZE_TRIP_SCHEMA.fields]

        metadata_fields = ["_source_file", "_source_month", "_ingestion_ts"]

        for field in metadata_fields:
            assert field in field_names, f"Missing metadata field: {field}"

    def test_bronze_has_partition_columns(self):
        """Verify bronze schema has partition columns."""
        from src.models.schemas import BRONZE_TRIP_SCHEMA

        field_names = [field.name for field in BRONZE_TRIP_SCHEMA.fields]

        assert "_year" in field_names
        assert "_month" in field_names


class TestDimensionalSchemas:
    """Tests for dimensional model schemas."""

    def test_fact_trips_schema_exists(self):
        """Verify fact_trips schema is defined."""
        from src.models.schemas import FACT_TRIPS_SCHEMA

        assert FACT_TRIPS_SCHEMA is not None
        assert len(FACT_TRIPS_SCHEMA.fields) > 0

    def test_fact_trips_has_surrogate_key(self):
        """Verify fact table has surrogate key."""
        from src.models.schemas import FACT_TRIPS_SCHEMA

        field_names = [field.name for field in FACT_TRIPS_SCHEMA.fields]
        assert "trip_key" in field_names

    def test_fact_trips_has_foreign_keys(self):
        """Verify fact table has foreign keys to dimensions."""
        from src.models.schemas import FACT_TRIPS_SCHEMA

        field_names = [field.name for field in FACT_TRIPS_SCHEMA.fields]

        foreign_keys = [
            "date_key",
            "vendor_key",
            "payment_type_key",
            "pickup_location_key",
            "dropoff_location_key",
            "ratecode_key",
        ]

        for fk in foreign_keys:
            assert fk in field_names, f"Missing foreign key: {fk}"

    def test_fact_trips_has_measures(self):
        """Verify fact table has measure columns."""
        from src.models.schemas import FACT_TRIPS_SCHEMA

        field_names = [field.name for field in FACT_TRIPS_SCHEMA.fields]

        measures = ["fare_amount", "total_amount", "passenger_count"]

        for measure in measures:
            assert measure in field_names, f"Missing measure: {measure}"

    def test_dimension_schemas_exist(self):
        """Verify all dimension schemas are defined."""
        from src.models.schemas import (
            DIM_DATE_SCHEMA,
            DIM_TIME_SCHEMA,
            DIM_LOCATION_SCHEMA,
            DIM_VENDOR_SCHEMA,
            DIM_RATECODE_SCHEMA,
            DIM_PAYMENT_TYPE_SCHEMA,
        )

        assert DIM_DATE_SCHEMA is not None
        assert DIM_TIME_SCHEMA is not None
        assert DIM_LOCATION_SCHEMA is not None
        assert DIM_VENDOR_SCHEMA is not None
        assert DIM_RATECODE_SCHEMA is not None
        assert DIM_PAYMENT_TYPE_SCHEMA is not None


class TestSchemaUtilities:
    """Tests for schema utility functions."""

    def test_get_schema_field_names(self):
        """Test field name extraction utility."""
        from src.models.schemas import get_schema_field_names, ZONE_LOOKUP_SCHEMA

        names = get_schema_field_names(ZONE_LOOKUP_SCHEMA)
        
        assert isinstance(names, list)
        assert "LocationID" in names
        assert len(names) == 4

    def test_schema_to_ddl(self):
        """Test DDL generation utility."""
        from src.models.schemas import schema_to_ddl, DIM_VENDOR_SCHEMA

        ddl = schema_to_ddl(DIM_VENDOR_SCHEMA, "dim_vendor")
        
        assert "CREATE TABLE dim_vendor" in ddl
        assert "vendor_key" in ddl
        assert "vendor_name" in ddl
