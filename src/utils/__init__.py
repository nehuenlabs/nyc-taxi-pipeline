"""
Utilities
=========

This module contains utility functions and configuration management.

Modules:
    - config: Environment-agnostic configuration management
    - spark_session: SparkSession factory
    - database: Database abstraction (PostgreSQL / BigQuery)
"""

from src.utils.config import (
    PipelineConfig,
    Environment,
    StorageConfig,
    PostgresConfig,
    BigQueryConfig,
    SparkConfig,
)
from src.utils.spark_session import (
    create_spark_session,
    get_or_create_spark_session,
    stop_spark_session,
    SparkSessionManager,
)
from src.utils.database import (
    DatabaseWriter,
    DatabaseReader,
    execute_sql,
    execute_sql_file,
    table_exists,
    get_table_row_count,
)

__all__ = [
    # Config
    "PipelineConfig",
    "Environment",
    "StorageConfig",
    "PostgresConfig",
    "BigQueryConfig",
    "SparkConfig",
    # Spark
    "create_spark_session",
    "get_or_create_spark_session",
    "stop_spark_session",
    "SparkSessionManager",
    # Database
    "DatabaseWriter",
    "DatabaseReader",
    "execute_sql",
    "execute_sql_file",
    "table_exists",
    "get_table_row_count",
]
