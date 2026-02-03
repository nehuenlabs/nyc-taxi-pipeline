#!/usr/bin/env python3
"""
Pipeline Monitoring Script
==========================

Generates a health report of the NYC Taxi pipeline, including:
- Database connection status
- Data quality metrics
- Row counts per table
- Recent ingestion stats

Usage:
    python scripts/monitor.py
    make monitor
"""

import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.monitoring import PipelineMonitor, get_logger

logger = get_logger("monitor", json_format=False)


def check_database_connection() -> dict:
    """Check PostgreSQL connection and return status."""
    try:
        import psycopg2
        from src.utils.config import PipelineConfig
        
        config = PipelineConfig.load()
        
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password,
        )
        conn.close()
        
        return {"status": "✅ Connected", "error": None}
    except ImportError:
        return {"status": "⚠️ psycopg2 not installed", "error": "pip install psycopg2-binary"}
    except Exception as e:
        return {"status": "❌ Failed", "error": str(e)}


def get_table_counts() -> dict:
    """Get row counts for all tables."""
    try:
        import psycopg2
        from src.utils.config import PipelineConfig
        
        config = PipelineConfig.load()
        
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password,
        )
        
        tables = [
            "nyc_taxi_dw.fact_trips",
            "nyc_taxi_dw.dim_date",
            "nyc_taxi_dw.dim_time",
            "nyc_taxi_dw.dim_location",
            "nyc_taxi_dw.dim_vendor",
            "nyc_taxi_dw.dim_payment_type",
            "nyc_taxi_dw.dim_ratecode",
        ]
        
        counts = {}
        cursor = conn.cursor()
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                counts[table.split(".")[-1]] = count
            except Exception:
                counts[table.split(".")[-1]] = "N/A"
        
        cursor.close()
        conn.close()
        
        return counts
    except Exception as e:
        return {"error": str(e)}


def get_data_quality_stats() -> dict:
    """Get data quality statistics from fact_trips."""
    try:
        import psycopg2
        from src.utils.config import PipelineConfig
        
        config = PipelineConfig.load()
        
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password,
        )
        
        cursor = conn.cursor()
        
        stats = {}
        
        # Total trips
        cursor.execute("SELECT COUNT(*) FROM nyc_taxi_dw.fact_trips")
        stats["total_trips"] = cursor.fetchone()[0]
        
        # Null fare amounts
        cursor.execute("""
            SELECT COUNT(*) FROM nyc_taxi_dw.fact_trips 
            WHERE fare_amount IS NULL
        """)
        stats["null_fares"] = cursor.fetchone()[0]
        
        # Negative fares
        cursor.execute("""
            SELECT COUNT(*) FROM nyc_taxi_dw.fact_trips 
            WHERE fare_amount < 0
        """)
        stats["negative_fares"] = cursor.fetchone()[0]
        
        # Average fare
        cursor.execute("""
            SELECT ROUND(AVG(fare_amount)::numeric, 2) 
            FROM nyc_taxi_dw.fact_trips 
            WHERE fare_amount > 0
        """)
        stats["avg_fare"] = cursor.fetchone()[0]
        
        # Date range (using dim_date join)
        cursor.execute("""
            SELECT 
                MIN(d.full_date)::date,
                MAX(d.full_date)::date
            FROM nyc_taxi_dw.fact_trips f
            JOIN nyc_taxi_dw.dim_date d ON f.date_key = d.date_key
        """)
        row = cursor.fetchone()
        stats["date_range"] = f"{row[0]} to {row[1]}" if row[0] else "N/A"
        
        cursor.close()
        conn.close()
        
        return stats
    except Exception as e:
        return {"error": str(e)}


def check_bronze_layer() -> dict:
    """Check bronze layer files."""
    from src.utils.config import PipelineConfig
    
    config = PipelineConfig.load()
    bronze_path = config.storage.bronze_path
    
    result = {
        "path": bronze_path,
        "trips_exists": False,
        "zones_exists": False,
    }
    
    import os
    
    trips_path = os.path.join(bronze_path, "trips")
    zones_path = os.path.join(bronze_path, "zones")
    
    result["trips_exists"] = os.path.exists(trips_path)
    result["zones_exists"] = os.path.exists(zones_path)
    
    if result["trips_exists"]:
        # Count parquet files
        files = [f for f in os.listdir(trips_path) if f.endswith(".parquet")]
        result["trip_files"] = len(files)
    
    return result


def print_report():
    """Print the full monitoring report."""
    print("\n" + "=" * 70)
    print("        NYC TAXI PIPELINE - MONITORING REPORT")
    print("=" * 70)
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Database Connection
    print("\n📊 DATABASE CONNECTION")
    print("-" * 70)
    db_status = check_database_connection()
    print(f"  Status: {db_status['status']}")
    if db_status.get("error"):
        print(f"  Error:  {db_status['error']}")
    
    # Table Counts
    print("\n📋 TABLE ROW COUNTS")
    print("-" * 70)
    counts = get_table_counts()
    if "error" not in counts:
        for table, count in counts.items():
            count_str = f"{count:,}" if isinstance(count, int) else count
            print(f"  {table:25} {count_str:>15}")
    else:
        print(f"  Error: {counts['error']}")
    
    # Data Quality
    print("\n🔍 DATA QUALITY METRICS")
    print("-" * 70)
    dq_stats = get_data_quality_stats()
    if "error" not in dq_stats:
        total = dq_stats.get("total_trips", 0)
        null_fares = dq_stats.get("null_fares", 0)
        neg_fares = dq_stats.get("negative_fares", 0)
        
        print(f"  Total Trips:        {total:,}")
        print(f"  Date Range:         {dq_stats.get('date_range', 'N/A')}")
        print(f"  Average Fare:       ${dq_stats.get('avg_fare', 0)}")
        print(f"  Null Fares:         {null_fares:,} ({null_fares/total*100:.2f}%)" if total else "  Null Fares: N/A")
        print(f"  Negative Fares:     {neg_fares:,} ({neg_fares/total*100:.2f}%)" if total else "  Negative Fares: N/A")
        
        # Quality Score
        if total > 0:
            issues = null_fares + neg_fares
            quality_pct = (1 - issues/total) * 100
            quality_status = "✅" if quality_pct >= 99 else "⚠️" if quality_pct >= 95 else "❌"
            print(f"\n  Quality Score:      {quality_status} {quality_pct:.2f}%")
    else:
        print(f"  Error: {dq_stats['error']}")
    
    # Bronze Layer
    print("\n📁 BRONZE LAYER STATUS")
    print("-" * 70)
    bronze = check_bronze_layer()
    print(f"  Path:          {bronze['path']}")
    print(f"  Trips Data:    {'✅ Present' if bronze['trips_exists'] else '❌ Missing'}")
    print(f"  Zones Data:    {'✅ Present' if bronze['zones_exists'] else '❌ Missing'}")
    if bronze.get("trip_files"):
        print(f"  Parquet Files: {bronze['trip_files']}")
    
    # Summary
    print("\n" + "=" * 70)
    all_good = (
        db_status['status'].startswith("✅") and 
        bronze['trips_exists'] and 
        bronze['zones_exists']
    )
    if all_good:
        print("  ✅ PIPELINE STATUS: HEALTHY")
    else:
        print("  ⚠️  PIPELINE STATUS: ISSUES DETECTED")
    print("=" * 70 + "\n")


def demo_pipeline_monitor():
    """Demo the PipelineMonitor class."""
    print("\n" + "=" * 70)
    print("        PIPELINE MONITOR DEMO")
    print("=" * 70)
    
    monitor = PipelineMonitor("demo_pipeline", json_logging=False)
    
    # Simulate pipeline stages
    with monitor.track_stage("extract") as stage:
        stage.records_out = 3000000
    
    with monitor.track_stage("transform") as stage:
        stage.records_in = 3000000
        stage.records_out = 2950000
        stage.records_rejected = 50000
    
    with monitor.track_stage("load") as stage:
        stage.records_in = 2950000
        stage.records_out = 2950000
    
    monitor.report()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="NYC Taxi Pipeline Monitor")
    parser.add_argument(
        "--demo", 
        action="store_true", 
        help="Run PipelineMonitor demo"
    )
    
    args = parser.parse_args()
    
    if args.demo:
        demo_pipeline_monitor()
    else:
        print_report()
