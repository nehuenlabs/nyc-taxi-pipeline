"""
NYC Taxi Pipeline CLI
=====================

Command-line interface for running pipeline jobs.

Usage:
    # Run bronze ingestion
    python -m src.cli bronze --months 2023-01 2023-02
    
    # Run dimensional transform
    python -m src.cli transform --months 2023-01 2023-02
    
    # Run full pipeline
    python -m src.cli full --months 2023-01 2023-02
"""

from __future__ import annotations

import logging
import sys
from typing import List, Optional

import click

from src.utils.config import PipelineConfig
from src.utils.spark_session import create_spark_session, stop_spark_session


# Configure logging
def setup_logging(level: str = "INFO") -> None:
    """Configure logging for CLI."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    default="INFO",
    help="Logging level"
)
@click.pass_context
def cli(ctx: click.Context, log_level: str) -> None:
    """NYC Taxi Pipeline - Data Processing CLI."""
    setup_logging(log_level)
    ctx.ensure_object(dict)
    ctx.obj["log_level"] = log_level


@cli.command()
@click.option(
    "--months",
    "-m",
    multiple=True,
    required=True,
    help="Months to process (format: YYYY-MM). Can specify multiple."
)
@click.option(
    "--no-download",
    is_flag=True,
    help="Skip downloading data (use existing files)"
)
@click.option(
    "--force-download",
    is_flag=True,
    help="Force re-download even if files exist"
)
@click.pass_context
def bronze(
    ctx: click.Context,
    months: tuple,
    no_download: bool,
    force_download: bool,
) -> None:
    """
    Run bronze ingestion job.
    
    Downloads raw data from NYC TLC and writes to Bronze layer.
    
    Example:
        python -m src.cli bronze --months 2023-01 --months 2023-02
    """
    from src.jobs.bronze_ingestion import run_bronze_ingestion
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Bronze Ingestion Job")
    
    # Load config
    config = PipelineConfig.load()
    
    # Create Spark session
    spark = create_spark_session(config, app_name="BronzeIngestion")
    
    try:
        stats = run_bronze_ingestion(
            config=config,
            spark=spark,
            months=list(months),
            download=not no_download,
            force_download=force_download,
        )
        
        click.echo("\n" + "=" * 50)
        click.echo(click.style("✅ Bronze Ingestion Complete", fg="green", bold=True))
        click.echo(f"   Months processed: {stats['months_processed']}")
        click.echo(f"   Total rows: {stats['total_rows']:,}")
        click.echo(f"   Zones loaded: {stats['zones_loaded']}")
        click.echo("=" * 50 + "\n")
        
    except Exception as e:
        click.echo(click.style(f"❌ Error: {e}", fg="red", bold=True))
        raise
    finally:
        stop_spark_session(spark)


@cli.command()
@click.option(
    "--months",
    "-m",
    multiple=True,
    required=True,
    help="Months to process (format: YYYY-MM). Can specify multiple."
)
@click.option(
    "--no-reload-dimensions",
    is_flag=True,
    help="Skip reloading dimension tables"
)
@click.option(
    "--fact-mode",
    type=click.Choice(["append", "overwrite"]),
    default="append",
    help="Write mode for fact table"
)
@click.pass_context
def transform(
    ctx: click.Context,
    months: tuple,
    no_reload_dimensions: bool,
    fact_mode: str,
) -> None:
    """
    Run dimensional transformation job.
    
    Transforms Bronze data to dimensional model in PostgreSQL.
    
    Example:
        python -m src.cli transform --months 2023-01 --months 2023-02
    """
    from src.jobs.dimensional_transform import run_dimensional_transform
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Dimensional Transform Job")
    
    # Load config
    config = PipelineConfig.load()
    
    # Create Spark session
    spark = create_spark_session(config, app_name="DimensionalTransform")
    
    try:
        stats = run_dimensional_transform(
            config=config,
            spark=spark,
            months=list(months),
            reload_dimensions=not no_reload_dimensions,
            fact_mode=fact_mode,
        )
        
        click.echo("\n" + "=" * 50)
        click.echo(click.style("✅ Dimensional Transform Complete", fg="green", bold=True))
        click.echo(f"   Trips read: {stats['trips_read']:,}")
        click.echo(f"   Trips after filter: {stats['trips_after_filter']:,}")
        click.echo(f"   Facts written: {stats['facts_written']:,}")
        click.echo(f"   Dimensions loaded: {len(stats['dimensions_loaded'])}")
        click.echo("=" * 50 + "\n")
        
    except Exception as e:
        click.echo(click.style(f"❌ Error: {e}", fg="red", bold=True))
        raise
    finally:
        stop_spark_session(spark)


@cli.command()
@click.option(
    "--months",
    "-m",
    multiple=True,
    required=True,
    help="Months to process (format: YYYY-MM). Can specify multiple."
)
@click.option(
    "--no-download",
    is_flag=True,
    help="Skip downloading data (use existing files)"
)
@click.option(
    "--force-download",
    is_flag=True,
    help="Force re-download even if files exist"
)
@click.option(
    "--fact-mode",
    type=click.Choice(["append", "overwrite"]),
    default="overwrite",
    help="Write mode for fact table"
)
@click.pass_context
def full(
    ctx: click.Context,
    months: tuple,
    no_download: bool,
    force_download: bool,
    fact_mode: str,
) -> None:
    """
    Run full pipeline (bronze + transform).
    
    Downloads data, ingests to Bronze, transforms to dimensional model.
    
    Example:
        python -m src.cli full --months 2023-01
    """
    from src.jobs.bronze_ingestion import run_bronze_ingestion
    from src.jobs.dimensional_transform import run_dimensional_transform
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Full Pipeline")
    
    # Load config
    config = PipelineConfig.load()
    
    # Create Spark session
    spark = create_spark_session(config, app_name="FullPipeline")
    
    try:
        # Step 1: Bronze Ingestion
        click.echo("\n" + "=" * 50)
        click.echo(click.style("Step 1/2: Bronze Ingestion", fg="blue", bold=True))
        click.echo("=" * 50 + "\n")
        
        bronze_stats = run_bronze_ingestion(
            config=config,
            spark=spark,
            months=list(months),
            download=not no_download,
            force_download=force_download,
        )
        
        # Step 2: Dimensional Transform
        click.echo("\n" + "=" * 50)
        click.echo(click.style("Step 2/2: Dimensional Transform", fg="blue", bold=True))
        click.echo("=" * 50 + "\n")
        
        transform_stats = run_dimensional_transform(
            config=config,
            spark=spark,
            months=list(months),
            reload_dimensions=True,
            fact_mode=fact_mode,
        )
        
        # Summary
        click.echo("\n" + "=" * 50)
        click.echo(click.style("✅ FULL PIPELINE COMPLETE", fg="green", bold=True))
        click.echo("=" * 50)
        click.echo(f"\n📥 Bronze Ingestion:")
        click.echo(f"   - Months processed: {bronze_stats['months_processed']}")
        click.echo(f"   - Raw rows ingested: {bronze_stats['total_rows']:,}")
        click.echo(f"\n📊 Dimensional Transform:")
        click.echo(f"   - Trips after quality filter: {transform_stats['trips_after_filter']:,}")
        click.echo(f"   - Facts written: {transform_stats['facts_written']:,}")
        click.echo(f"   - Dimensions loaded: {len(transform_stats['dimensions_loaded'])}")
        click.echo("\n" + "=" * 50 + "\n")
        
    except Exception as e:
        click.echo(click.style(f"❌ Error: {e}", fg="red", bold=True))
        raise
    finally:
        stop_spark_session(spark)


@cli.command()
@click.pass_context
def info(ctx: click.Context) -> None:
    """Show pipeline configuration info."""
    config = PipelineConfig.load()
    
    click.echo("\n" + "=" * 50)
    click.echo(click.style("NYC Taxi Pipeline Configuration", fg="blue", bold=True))
    click.echo("=" * 50)
    click.echo(f"\n🌍 Environment: {config.environment.value}")
    click.echo(f"\n📂 Storage:")
    click.echo(f"   - Raw:    {config.storage.raw_path}")
    click.echo(f"   - Bronze: {config.storage.bronze_path}")
    click.echo(f"   - Silver: {config.storage.silver_path}")
    click.echo(f"   - Gold:   {config.storage.gold_path}")
    click.echo(f"\n🗄️ Database:")
    click.echo(f"   - Type: {config.database.db_type}")
    if config.database.postgres:
        click.echo(f"   - Host: {config.database.postgres.host}")
        click.echo(f"   - Port: {config.database.postgres.port}")
        click.echo(f"   - Database: {config.database.postgres.database}")
    click.echo(f"\n⚡ Spark:")
    click.echo(f"   - Master: {config.spark.master}")
    click.echo(f"   - Driver Memory: {config.spark.driver_memory}")
    click.echo(f"   - Executor Memory: {config.spark.executor_memory}")
    click.echo("\n" + "=" * 50 + "\n")


def main() -> None:
    """Main entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
