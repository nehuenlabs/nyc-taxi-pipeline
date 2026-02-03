"""
Pipeline Monitoring and Metrics
===============================

Provides structured logging, metrics collection, and alerting capabilities
for production-grade pipeline monitoring.

Features:
    - Structured JSON logging
    - Pipeline execution metrics
    - Data quality metrics
    - Simple alerting via logging (extensible to Slack, PagerDuty, etc.)
    
Usage:
    from src.monitoring import PipelineMonitor, get_logger
    
    logger = get_logger(__name__)
    monitor = PipelineMonitor("bronze_ingestion")
    
    with monitor.track_stage("download"):
        # ... download logic
    
    monitor.report()
"""

import json
import logging
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


# =============================================================================
# Structured Logging
# =============================================================================

class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    Outputs logs in JSON format for easy parsing by log aggregators
    (e.g., CloudWatch, Stackdriver, ELK).
    """
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields if present
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)
        
        return json.dumps(log_data)


def get_logger(
    name: str,
    level: int = logging.INFO,
    json_format: bool = True
) -> logging.Logger:
    """
    Get a configured logger with structured output.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level
        json_format: If True, output JSON; otherwise, human-readable
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        
        if json_format:
            handler.setFormatter(JSONFormatter())
        else:
            handler.setFormatter(logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
            ))
        
        logger.addHandler(handler)
        logger.setLevel(level)
    
    return logger


# =============================================================================
# Metrics Collection
# =============================================================================

class MetricType(str, Enum):
    """Types of metrics that can be collected."""
    COUNTER = "counter"
    GAUGE = "gauge"
    TIMER = "timer"
    HISTOGRAM = "histogram"


@dataclass
class Metric:
    """A single metric measurement."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "type": self.metric_type.value,
            "timestamp": self.timestamp.isoformat() + "Z",
            "tags": self.tags,
        }


@dataclass
class StageMetrics:
    """Metrics for a pipeline stage."""
    name: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    records_in: int = 0
    records_out: int = 0
    records_rejected: int = 0
    bytes_processed: int = 0
    error: Optional[str] = None
    
    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success(self) -> bool:
        return self.error is None
    
    @property
    def rejection_rate(self) -> float:
        total = self.records_in
        if total == 0:
            return 0.0
        return self.records_rejected / total
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage": self.name,
            "duration_seconds": self.duration_seconds,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "records_rejected": self.records_rejected,
            "rejection_rate": round(self.rejection_rate, 4),
            "bytes_processed": self.bytes_processed,
            "success": self.success,
            "error": self.error,
        }


# =============================================================================
# Pipeline Monitor
# =============================================================================

class AlertLevel(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Alert:
    """An alert triggered by the pipeline."""
    level: AlertLevel
    message: str
    stage: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    details: Dict[str, Any] = field(default_factory=dict)


class PipelineMonitor:
    """
    Monitors pipeline execution, collects metrics, and triggers alerts.
    
    Usage:
        monitor = PipelineMonitor("my_pipeline")
        
        with monitor.track_stage("extract") as stage:
            data = extract_data()
            stage.records_out = len(data)
        
        with monitor.track_stage("transform") as stage:
            stage.records_in = len(data)
            result = transform(data)
            stage.records_out = len(result)
        
        monitor.report()
    """
    
    # Thresholds for alerting
    REJECTION_RATE_WARNING = 0.05  # 5%
    REJECTION_RATE_CRITICAL = 0.20  # 20%
    
    def __init__(
        self,
        pipeline_name: str,
        run_id: Optional[str] = None,
        json_logging: bool = True
    ):
        self.pipeline_name = pipeline_name
        self.run_id = run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.start_time = datetime.utcnow()
        self.end_time: Optional[datetime] = None
        
        self.stages: List[StageMetrics] = []
        self.metrics: List[Metric] = []
        self.alerts: List[Alert] = []
        
        self.logger = get_logger(
            f"pipeline.{pipeline_name}",
            json_format=json_logging
        )
        
        self.logger.info(
            f"Pipeline '{pipeline_name}' started",
            extra={"extra_fields": {
                "event": "pipeline_start",
                "pipeline": pipeline_name,
                "run_id": self.run_id,
            }}
        )
    
    @contextmanager
    def track_stage(self, stage_name: str):
        """
        Context manager to track a pipeline stage.
        
        Args:
            stage_name: Name of the stage being tracked
        
        Yields:
            StageMetrics object to record metrics
        """
        stage = StageMetrics(name=stage_name)
        stage.start_time = datetime.utcnow()
        
        self.logger.info(
            f"Stage '{stage_name}' started",
            extra={"extra_fields": {
                "event": "stage_start",
                "stage": stage_name,
                "run_id": self.run_id,
            }}
        )
        
        try:
            yield stage
            stage.end_time = datetime.utcnow()
            
            # Check for data quality issues
            self._check_stage_alerts(stage)
            
            self.logger.info(
                f"Stage '{stage_name}' completed in {stage.duration_seconds:.2f}s",
                extra={"extra_fields": {
                    "event": "stage_complete",
                    "stage": stage_name,
                    "run_id": self.run_id,
                    "metrics": stage.to_dict(),
                }}
            )
            
        except Exception as e:
            stage.end_time = datetime.utcnow()
            stage.error = str(e)
            
            self.logger.error(
                f"Stage '{stage_name}' failed: {e}",
                extra={"extra_fields": {
                    "event": "stage_error",
                    "stage": stage_name,
                    "run_id": self.run_id,
                    "error": str(e),
                }}
            )
            
            self._add_alert(
                AlertLevel.CRITICAL,
                f"Stage '{stage_name}' failed: {e}",
                stage_name,
                {"error_type": type(e).__name__}
            )
            
            raise
        
        finally:
            self.stages.append(stage)
    
    def _check_stage_alerts(self, stage: StageMetrics) -> None:
        """Check stage metrics and trigger alerts if thresholds exceeded."""
        
        # Check rejection rate
        if stage.rejection_rate >= self.REJECTION_RATE_CRITICAL:
            self._add_alert(
                AlertLevel.CRITICAL,
                f"High rejection rate: {stage.rejection_rate:.1%}",
                stage.name,
                {"rejection_rate": stage.rejection_rate}
            )
        elif stage.rejection_rate >= self.REJECTION_RATE_WARNING:
            self._add_alert(
                AlertLevel.WARNING,
                f"Elevated rejection rate: {stage.rejection_rate:.1%}",
                stage.name,
                {"rejection_rate": stage.rejection_rate}
            )
        
        # Check for no output records
        if stage.records_in > 0 and stage.records_out == 0:
            self._add_alert(
                AlertLevel.WARNING,
                "Stage produced no output records",
                stage.name,
                {"records_in": stage.records_in}
            )
    
    def _add_alert(
        self,
        level: AlertLevel,
        message: str,
        stage: str,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add an alert and log it."""
        alert = Alert(
            level=level,
            message=message,
            stage=stage,
            details=details or {}
        )
        self.alerts.append(alert)
        
        log_method = {
            AlertLevel.INFO: self.logger.info,
            AlertLevel.WARNING: self.logger.warning,
            AlertLevel.CRITICAL: self.logger.critical,
        }[level]
        
        log_method(
            f"ALERT [{level.value.upper()}]: {message}",
            extra={"extra_fields": {
                "event": "alert",
                "alert_level": level.value,
                "stage": stage,
                "run_id": self.run_id,
                **details,
            }}
        )
    
    def record_metric(
        self,
        name: str,
        value: float,
        metric_type: MetricType = MetricType.GAUGE,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Record a custom metric.
        
        Args:
            name: Metric name
            value: Metric value
            metric_type: Type of metric
            tags: Optional tags for the metric
        """
        metric = Metric(
            name=name,
            value=value,
            metric_type=metric_type,
            tags=tags or {}
        )
        self.metrics.append(metric)
    
    def finish(self) -> Dict[str, Any]:
        """
        Finish pipeline monitoring and return summary.
        
        Returns:
            Dictionary with pipeline execution summary
        """
        self.end_time = datetime.utcnow()
        total_duration = (self.end_time - self.start_time).total_seconds()
        
        # Aggregate metrics
        total_records_in = sum(s.records_in for s in self.stages)
        total_records_out = sum(s.records_out for s in self.stages)
        total_rejected = sum(s.records_rejected for s in self.stages)
        failed_stages = [s for s in self.stages if not s.success]
        
        summary = {
            "pipeline": self.pipeline_name,
            "run_id": self.run_id,
            "start_time": self.start_time.isoformat() + "Z",
            "end_time": self.end_time.isoformat() + "Z",
            "duration_seconds": round(total_duration, 2),
            "success": len(failed_stages) == 0,
            "stages_total": len(self.stages),
            "stages_failed": len(failed_stages),
            "total_records_in": total_records_in,
            "total_records_out": total_records_out,
            "total_records_rejected": total_rejected,
            "alerts_count": len(self.alerts),
            "alerts_by_level": {
                level.value: len([a for a in self.alerts if a.level == level])
                for level in AlertLevel
            },
            "stages": [s.to_dict() for s in self.stages],
        }
        
        self.logger.info(
            f"Pipeline '{self.pipeline_name}' finished in {total_duration:.2f}s",
            extra={"extra_fields": {
                "event": "pipeline_complete",
                "run_id": self.run_id,
                "summary": summary,
            }}
        )
        
        return summary
    
    def report(self) -> None:
        """Print a human-readable report of the pipeline execution."""
        summary = self.finish()
        
        print("\n" + "=" * 60)
        print(f"PIPELINE EXECUTION REPORT: {self.pipeline_name}")
        print("=" * 60)
        print(f"Run ID:      {summary['run_id']}")
        print(f"Duration:    {summary['duration_seconds']}s")
        print(f"Status:      {'✅ SUCCESS' if summary['success'] else '❌ FAILED'}")
        print("-" * 60)
        print("STAGES:")
        for stage in summary['stages']:
            status = "✅" if stage['success'] else "❌"
            print(f"  {status} {stage['stage']}: {stage['duration_seconds']:.2f}s "
                  f"({stage['records_in']} → {stage['records_out']} records)")
            if stage['records_rejected'] > 0:
                print(f"      ⚠️  Rejected: {stage['records_rejected']} "
                      f"({stage['rejection_rate']:.1%})")
        print("-" * 60)
        print(f"TOTALS:")
        print(f"  Records In:       {summary['total_records_in']:,}")
        print(f"  Records Out:      {summary['total_records_out']:,}")
        print(f"  Records Rejected: {summary['total_records_rejected']:,}")
        print("-" * 60)
        print(f"ALERTS: {summary['alerts_count']}")
        for level, count in summary['alerts_by_level'].items():
            if count > 0:
                print(f"  {level.upper()}: {count}")
        print("=" * 60 + "\n")


# =============================================================================
# Data Quality Metrics
# =============================================================================

class DataQualityMonitor:
    """
    Monitors data quality metrics for a DataFrame.
    
    Usage:
        dq = DataQualityMonitor(df, "trips_data")
        dq.check_nulls(["trip_distance", "fare_amount"])
        dq.check_range("fare_amount", min_val=0, max_val=1000)
        dq.check_uniqueness("trip_id")
        
        report = dq.get_report()
    """
    
    def __init__(self, df, dataset_name: str):
        """
        Initialize data quality monitor.
        
        Args:
            df: PySpark DataFrame to monitor
            dataset_name: Name for reporting
        """
        self.df = df
        self.dataset_name = dataset_name
        self.total_rows = df.count()
        self.checks: List[Dict[str, Any]] = []
        self.logger = get_logger(f"dq.{dataset_name}")
    
    def check_nulls(
        self,
        columns: List[str],
        threshold: float = 0.0
    ) -> "DataQualityMonitor":
        """
        Check null percentage in columns.
        
        Args:
            columns: Columns to check
            threshold: Maximum acceptable null percentage (0.0 to 1.0)
        
        Returns:
            Self for chaining
        """
        from pyspark.sql import functions as F
        
        for col in columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            null_pct = null_count / self.total_rows if self.total_rows > 0 else 0
            passed = null_pct <= threshold
            
            self.checks.append({
                "check": "null_check",
                "column": col,
                "null_count": null_count,
                "null_percentage": round(null_pct, 4),
                "threshold": threshold,
                "passed": passed,
            })
            
            if not passed:
                self.logger.warning(
                    f"Null check failed for '{col}': {null_pct:.2%} > {threshold:.2%}"
                )
        
        return self
    
    def check_range(
        self,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None
    ) -> "DataQualityMonitor":
        """
        Check if values are within expected range.
        
        Args:
            column: Column to check
            min_val: Minimum acceptable value
            max_val: Maximum acceptable value
        
        Returns:
            Self for chaining
        """
        from pyspark.sql import functions as F
        
        conditions = []
        if min_val is not None:
            conditions.append(F.col(column) < min_val)
        if max_val is not None:
            conditions.append(F.col(column) > max_val)
        
        if conditions:
            combined = conditions[0]
            for c in conditions[1:]:
                combined = combined | c
            
            out_of_range = self.df.filter(combined).count()
        else:
            out_of_range = 0
        
        out_of_range_pct = out_of_range / self.total_rows if self.total_rows > 0 else 0
        
        self.checks.append({
            "check": "range_check",
            "column": column,
            "min_val": min_val,
            "max_val": max_val,
            "out_of_range_count": out_of_range,
            "out_of_range_percentage": round(out_of_range_pct, 4),
            "passed": out_of_range == 0,
        })
        
        if out_of_range > 0:
            self.logger.warning(
                f"Range check: {out_of_range} records out of range for '{column}'"
            )
        
        return self
    
    def check_uniqueness(self, column: str) -> "DataQualityMonitor":
        """
        Check if column values are unique.
        
        Args:
            column: Column to check
        
        Returns:
            Self for chaining
        """
        distinct_count = self.df.select(column).distinct().count()
        duplicate_count = self.total_rows - distinct_count
        is_unique = duplicate_count == 0
        
        self.checks.append({
            "check": "uniqueness_check",
            "column": column,
            "total_rows": self.total_rows,
            "distinct_count": distinct_count,
            "duplicate_count": duplicate_count,
            "passed": is_unique,
        })
        
        if not is_unique:
            self.logger.warning(
                f"Uniqueness check failed for '{column}': {duplicate_count} duplicates"
            )
        
        return self
    
    def get_report(self) -> Dict[str, Any]:
        """Get data quality report."""
        passed_checks = sum(1 for c in self.checks if c["passed"])
        
        return {
            "dataset": self.dataset_name,
            "total_rows": self.total_rows,
            "checks_total": len(self.checks),
            "checks_passed": passed_checks,
            "checks_failed": len(self.checks) - passed_checks,
            "all_passed": passed_checks == len(self.checks),
            "checks": self.checks,
        }
