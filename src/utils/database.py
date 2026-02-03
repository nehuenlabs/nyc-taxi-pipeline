"""
Database Utilities
==================

Abstraction layer for database operations.
Supports PostgreSQL (local/GCP) and BigQuery (GCP).

Usage:
    from src.utils.config import PipelineConfig
    from src.utils.database import DatabaseWriter
    
    config = PipelineConfig.load()
    writer = DatabaseWriter(config)
    
    # Write DataFrame to database
    writer.write_dataframe(df, "fact_trips")
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from src.utils.config import PipelineConfig

logger = logging.getLogger(__name__)


class DatabaseWriter:
    """
    Writes DataFrames to the configured database.
    
    Supports:
        - PostgreSQL via JDBC
        - BigQuery via Spark connector (GCP only)
    """
    
    def __init__(self, config: PipelineConfig):
        """
        Initialize DatabaseWriter.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.postgres = config.postgres
        self.bigquery = config.bigquery
    
    def write_to_postgres(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        partition_column: Optional[str] = None,
    ) -> None:
        """
        Write DataFrame to PostgreSQL table.
        
        Args:
            df: Spark DataFrame to write
            table_name: Target table name (will be prefixed with schema)
            mode: Write mode ("overwrite", "append", "error", "ignore")
            partition_column: Optional column for partitioned writes
        
        Note:
            For "overwrite" mode, uses TRUNCATE CASCADE + append to avoid
            FK constraint issues (Spark's default DROP fails with FKs).
        """
        full_table_name = f"{self.postgres.schema}.{table_name}"
        
        logger.info(f"Writing to PostgreSQL: {full_table_name}")
        
        row_count = df.count()
        logger.info(f"Mode: {mode}, Rows: {row_count}")
        
        # Handle overwrite mode specially to avoid FK issues
        actual_mode = mode
        if mode == "overwrite":
            # Use TRUNCATE CASCADE instead of DROP to preserve table structure and FKs
            self._truncate_table(table_name)
            actual_mode = "append"  # After truncate, just append
        
        writer = (
            df.write
            .format("jdbc")
            .option("url", self.postgres.jdbc_url)
            .option("dbtable", full_table_name)
            .option("user", self.postgres.user)
            .option("password", self.postgres.password)
            .option("driver", "org.postgresql.Driver")
            .mode(actual_mode)
        )
        
        # Add batch size for better performance
        writer = writer.option("batchsize", 10000)
        
        # Execute write
        writer.save()
        
        logger.info(f"Successfully wrote to {full_table_name}")
    
    def _truncate_table(self, table_name: str) -> None:
        """
        Truncate a table using CASCADE to handle FK constraints.
        
        Args:
            table_name: Table name (without schema)
        """
        import psycopg2
        
        full_table_name = f"{self.postgres.schema}.{table_name}"
        logger.info(f"Truncating table: {full_table_name}")
        
        conn = psycopg2.connect(
            host=self.postgres.host,
            port=self.postgres.port,
            database=self.postgres.database,
            user=self.postgres.user,
            password=self.postgres.password,
        )
        
        try:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {full_table_name} CASCADE")
            conn.commit()
            logger.info(f"Truncated {full_table_name}")
        finally:
            conn.close()
    
    def write_to_bigquery(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        partition_column: Optional[str] = None,
    ) -> None:
        """
        Write DataFrame to BigQuery table.
        
        Args:
            df: Spark DataFrame to write
            table_name: Target table name
            mode: Write mode ("overwrite", "append", "error", "ignore")
            partition_column: Optional column for partitioning
        
        Raises:
            ValueError: If BigQuery is not configured
        """
        if self.bigquery is None:
            raise ValueError("BigQuery is not configured. Set GCP_PROJECT_ID and BQ_DATASET.")
        
        full_table_name = f"{self.bigquery.full_dataset_id}.{table_name}"
        
        logger.info(f"Writing to BigQuery: {full_table_name}")
        logger.info(f"Mode: {mode}")
        
        writer = (
            df.write
            .format("bigquery")
            .option("table", full_table_name)
            .mode(mode)
        )
        
        # Add partitioning if specified
        if partition_column:
            writer = (
                writer
                .option("partitionField", partition_column)
                .option("partitionType", "DAY")
            )
        
        # Execute write
        writer.save()
        
        logger.info(f"Successfully wrote to {full_table_name}")
    
    def write_dataframe(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        partition_column: Optional[str] = None,
        target: str = "auto",
    ) -> None:
        """
        Write DataFrame to the appropriate database.
        
        Args:
            df: Spark DataFrame to write
            table_name: Target table name
            mode: Write mode ("overwrite", "append", "error", "ignore")
            partition_column: Optional column for partitioning
            target: Target database ("auto", "postgres", "bigquery")
                   "auto" uses PostgreSQL for local, BigQuery for GCP
        """
        if target == "auto":
            if self.config.is_local:
                target = "postgres"
            else:
                target = "bigquery" if self.bigquery else "postgres"
        
        if target == "postgres":
            self.write_to_postgres(df, table_name, mode, partition_column)
        elif target == "bigquery":
            self.write_to_bigquery(df, table_name, mode, partition_column)
        else:
            raise ValueError(f"Unknown target: {target}")


class DatabaseReader:
    """
    Reads data from the configured database.
    """
    
    def __init__(self, config: PipelineConfig, spark: SparkSession):
        """
        Initialize DatabaseReader.
        
        Args:
            config: Pipeline configuration
            spark: Active SparkSession
        """
        self.config = config
        self.spark = spark
        self.postgres = config.postgres
        self.bigquery = config.bigquery
    
    def read_from_postgres(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        predicate: Optional[str] = None,
    ) -> DataFrame:
        """
        Read DataFrame from PostgreSQL table.
        
        Args:
            table_name: Table name (will be prefixed with schema)
            columns: Optional list of columns to select
            predicate: Optional WHERE clause predicate
        
        Returns:
            Spark DataFrame
        """
        full_table_name = f"{self.postgres.schema}.{table_name}"
        
        logger.info(f"Reading from PostgreSQL: {full_table_name}")
        
        reader = (
            self.spark.read
            .format("jdbc")
            .option("url", self.postgres.jdbc_url)
            .option("dbtable", full_table_name)
            .option("user", self.postgres.user)
            .option("password", self.postgres.password)
            .option("driver", "org.postgresql.Driver")
        )
        
        # Add predicate pushdown if specified
        if predicate:
            reader = reader.option("predicates", predicate)
        
        df = reader.load()
        
        # Select specific columns if specified
        if columns:
            df = df.select(columns)
        
        logger.info(f"Read {df.count()} rows from {full_table_name}")
        
        return df
    
    def read_from_bigquery(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filter: Optional[str] = None,
    ) -> DataFrame:
        """
        Read DataFrame from BigQuery table.
        
        Args:
            table_name: Table name
            columns: Optional list of columns to select
            filter: Optional filter expression
        
        Returns:
            Spark DataFrame
        
        Raises:
            ValueError: If BigQuery is not configured
        """
        if self.bigquery is None:
            raise ValueError("BigQuery is not configured.")
        
        full_table_name = f"{self.bigquery.full_dataset_id}.{table_name}"
        
        logger.info(f"Reading from BigQuery: {full_table_name}")
        
        reader = (
            self.spark.read
            .format("bigquery")
            .option("table", full_table_name)
        )
        
        # Add filter if specified
        if filter:
            reader = reader.option("filter", filter)
        
        df = reader.load()
        
        # Select specific columns if specified
        if columns:
            df = df.select(columns)
        
        return df


def execute_sql(
    config: PipelineConfig,
    sql: str,
) -> None:
    """
    Execute SQL statement against PostgreSQL.
    
    Useful for DDL operations (CREATE TABLE, etc.)
    
    Args:
        config: Pipeline configuration
        sql: SQL statement to execute
    """
    import psycopg2
    
    logger.info(f"Executing SQL: {sql[:100]}...")
    
    conn = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password,
    )
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("SQL executed successfully")
    finally:
        conn.close()


def execute_sql_file(
    config: PipelineConfig,
    file_path: str,
) -> None:
    """
    Execute SQL file against PostgreSQL.
    
    Args:
        config: Pipeline configuration
        file_path: Path to SQL file
    """
    with open(file_path, "r") as f:
        sql = f.read()
    
    execute_sql(config, sql)


def table_exists(
    config: PipelineConfig,
    table_name: str,
) -> bool:
    """
    Check if a table exists in PostgreSQL.
    
    Args:
        config: Pipeline configuration
        table_name: Table name (without schema)
    
    Returns:
        True if table exists, False otherwise
    """
    import psycopg2
    
    conn = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password,
    )
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s
                    AND table_name = %s
                )
                """,
                (config.postgres.schema, table_name)
            )
            result = cur.fetchone()[0]
        return result
    finally:
        conn.close()


def get_table_row_count(
    config: PipelineConfig,
    table_name: str,
) -> int:
    """
    Get row count for a PostgreSQL table.
    
    Args:
        config: Pipeline configuration
        table_name: Table name (without schema)
    
    Returns:
        Number of rows in the table
    """
    import psycopg2
    
    conn = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password,
    )
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {config.postgres.schema}.{table_name}"
            )
            result = cur.fetchone()[0]
        return result
    finally:
        conn.close()
