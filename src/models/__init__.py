"""
Data Models
===========

This module contains data schema definitions and dimensional model specifications.

Modules:
    - schemas: PySpark StructType definitions for raw and processed data
    - dimensional: Dimensional model definitions (facts and dimensions)
"""

from src.models.schemas import (
    YELLOW_TAXI_SCHEMA,
    ZONE_LOOKUP_SCHEMA,
    BRONZE_TRIP_SCHEMA,
    DIM_DATE_SCHEMA,
    DIM_TIME_SCHEMA,
    DIM_LOCATION_SCHEMA,
    DIM_VENDOR_SCHEMA,
    DIM_RATECODE_SCHEMA,
    DIM_PAYMENT_TYPE_SCHEMA,
    FACT_TRIPS_SCHEMA,
    get_schema_field_names,
    schema_to_ddl,
)
from src.models.dimensional import (
    VENDORS,
    RATE_CODES,
    PAYMENT_TYPES,
    generate_dim_date,
    generate_dim_time,
    generate_dim_vendor,
    generate_dim_ratecode,
    generate_dim_payment_type,
    generate_dim_location,
    transform_to_fact_trips,
    apply_quality_filters,
    DimensionalModel,
    MODEL,
)

__all__ = [
    # Schemas
    "YELLOW_TAXI_SCHEMA",
    "ZONE_LOOKUP_SCHEMA",
    "BRONZE_TRIP_SCHEMA",
    "DIM_DATE_SCHEMA",
    "DIM_TIME_SCHEMA",
    "DIM_LOCATION_SCHEMA",
    "DIM_VENDOR_SCHEMA",
    "DIM_RATECODE_SCHEMA",
    "DIM_PAYMENT_TYPE_SCHEMA",
    "FACT_TRIPS_SCHEMA",
    "get_schema_field_names",
    "schema_to_ddl",
    # Reference data
    "VENDORS",
    "RATE_CODES",
    "PAYMENT_TYPES",
    # Dimension generators
    "generate_dim_date",
    "generate_dim_time",
    "generate_dim_vendor",
    "generate_dim_ratecode",
    "generate_dim_payment_type",
    "generate_dim_location",
    # Fact transformation
    "transform_to_fact_trips",
    "apply_quality_filters",
    # Model metadata
    "DimensionalModel",
    "MODEL",
]
