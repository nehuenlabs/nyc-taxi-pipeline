"""
PySpark Jobs
============

This module contains the PySpark jobs for the NYC Taxi pipeline.

Jobs:
    - bronze_ingestion: Ingests raw data into the Bronze layer
    - dimensional_transform: Transforms Bronze data into dimensional model
"""

from src.jobs.bronze_ingestion import (
    run_bronze_ingestion,
    download_trip_data,
    download_zone_lookup,
    read_raw_trips,
    read_zone_lookup,
    add_bronze_metadata,
    write_bronze_trips,
    write_bronze_zones,
)
from src.jobs.dimensional_transform import (
    run_dimensional_transform,
    read_bronze_trips,
    read_bronze_zones,
    load_dimension_to_postgres,
    load_fact_to_postgres,
)

__all__ = [
    # Bronze Ingestion
    "run_bronze_ingestion",
    "download_trip_data",
    "download_zone_lookup",
    "read_raw_trips",
    "read_zone_lookup",
    "add_bronze_metadata",
    "write_bronze_trips",
    "write_bronze_zones",
    # Dimensional Transform
    "run_dimensional_transform",
    "read_bronze_trips",
    "read_bronze_zones",
    "load_dimension_to_postgres",
    "load_fact_to_postgres",
]
