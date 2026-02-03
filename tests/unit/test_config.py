"""Unit tests for configuration module."""

import os
import pytest


class TestPipelineConfig:
    """Tests for pipeline configuration."""

    def test_config_class_exists(self):
        """Verify config class can be imported."""
        from src.utils.config import PipelineConfig
        
        assert PipelineConfig is not None

    def test_load_returns_config(self):
        """Test that load() returns a config object."""
        from src.utils.config import PipelineConfig
        
        config = PipelineConfig.load()
        assert config is not None
        assert hasattr(config, "environment")

    def test_default_environment_is_local(self):
        """Test default environment is local."""
        from src.utils.config import PipelineConfig, Environment
        
        # Clear env var to test default
        env_backup = os.environ.get("PIPELINE_ENV")
        os.environ.pop("PIPELINE_ENV", None)
        
        try:
            config = PipelineConfig.load()
            assert config.environment == Environment.LOCAL
        finally:
            if env_backup:
                os.environ["PIPELINE_ENV"] = env_backup

    def test_is_local_property(self):
        """Test is_local property."""
        from src.utils.config import PipelineConfig
        
        os.environ.pop("PIPELINE_ENV", None)
        config = PipelineConfig.load()
        assert config.is_local is True


class TestStorageConfig:
    """Tests for storage configuration."""

    def test_storage_paths_configured(self):
        """Verify storage paths are set."""
        from src.utils.config import PipelineConfig
        
        config = PipelineConfig.load()
        
        assert config.storage.bronze_path is not None
        assert config.storage.gold_path is not None
        assert config.storage.raw_path is not None

    def test_local_storage_paths(self):
        """Test local storage path format."""
        from src.utils.config import StorageConfig
        
        storage = StorageConfig.for_local("/data")
        
        assert storage.bronze_path == "/data/bronze"
        assert storage.gold_path == "/data/gold"

    def test_gcp_storage_paths(self):
        """Test GCP storage path format."""
        from src.utils.config import StorageConfig
        
        storage = StorageConfig.for_gcp("my-bucket")
        
        assert storage.bronze_path == "gs://my-bucket/bronze"
        assert storage.gold_path == "gs://my-bucket/gold"

    def test_trips_bronze_path(self):
        """Test trips bronze path helper."""
        from src.utils.config import StorageConfig
        
        storage = StorageConfig.for_local("/data")
        assert storage.get_trips_bronze_path() == "/data/bronze/trips"


class TestPostgresConfig:
    """Tests for PostgreSQL configuration."""

    def test_postgres_defaults(self):
        """Test PostgreSQL default configuration."""
        from src.utils.config import PostgresConfig
        
        pg = PostgresConfig.from_env()
        
        assert pg.host is not None
        assert pg.port == 5432 or isinstance(pg.port, int)
        assert pg.database is not None

    def test_jdbc_url_format(self):
        """Test JDBC URL generation."""
        from src.utils.config import PostgresConfig
        
        pg = PostgresConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="user",
            password="pass"
        )
        
        assert "jdbc:postgresql://" in pg.jdbc_url
        assert "localhost:5432" in pg.jdbc_url
        assert "testdb" in pg.jdbc_url

    def test_connection_properties(self):
        """Test connection properties dict."""
        from src.utils.config import PostgresConfig
        
        pg = PostgresConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="myuser",
            password="mypass"
        )
        
        props = pg.connection_properties
        assert props["user"] == "myuser"
        assert props["password"] == "mypass"
        assert props["driver"] == "org.postgresql.Driver"


class TestSparkConfig:
    """Tests for Spark configuration."""

    def test_local_spark_config(self):
        """Test local Spark configuration."""
        from src.utils.config import SparkConfig
        
        spark = SparkConfig.for_local("TestApp")
        
        assert spark.app_name == "TestApp"
        assert spark.master is not None
        assert spark.driver_memory is not None

    def test_spark_extra_configs(self):
        """Test Spark extra configurations."""
        from src.utils.config import SparkConfig
        
        spark = SparkConfig.for_local()
        
        assert isinstance(spark.extra_configs, dict)
        assert "spark.sql.adaptive.enabled" in spark.extra_configs


class TestDataUrls:
    """Tests for NYC TLC data URLs."""

    def test_trip_data_url(self):
        """Test trip data URL generation."""
        from src.utils.config import get_trip_data_url
        
        url = get_trip_data_url("2023-01")
        
        assert "yellow_tripdata_2023-01.parquet" in url
        assert url.startswith("https://")

    def test_zone_lookup_url(self):
        """Test zone lookup URL."""
        from src.utils.config import get_zone_lookup_url
        
        url = get_zone_lookup_url()
        
        assert "taxi_zone_lookup.csv" in url
        assert url.startswith("https://")
