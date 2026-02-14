# ==============================================================================
# MIGRATED FROM AWS GLUE TO DATABRICKS
# Framework: Glue2Lakehouse v4.0 (Production Ready)
# ==============================================================================
#
# Review and test before production use
#
# Transformations:
#   - Removed Job class
#   - Converted DynamicFrame -> DataFrame
#   - Updated type annotations
#   - Syntax: PASSED
#
# ==============================================================================


"""
Logging Utilities for Glue Jobs
Provides structured logging with CloudWatch integration.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import logging
import sys
import json
from datetime import datetime
from typing import Any, Dict, Optional

def setup_logging(job_name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Setup structured logging for Glue job.

    Args:
        job_name: Name of the Glue job
        log_level: Logging level

    Returns:
        Configured logger
    """
    logger = logging.getLogger(job_name)
    logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers
    logger.handlers = []

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, log_level.upper()))

    # Create formatter
    formatter = StructuredFormatter(job_name)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)

class StructuredFormatter(logging.Formatter):
    """
    JSON structured log formatter for CloudWatch.
    """

    def __init__(self, job_name: str):
        super().__init__()
        self.job_name = job_name

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "job_name": self.job_name,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        if hasattr(record, 'extra'):
            log_entry.update(record.extra)

        return json.dumps(log_entry)

class JobMetrics:
    """
    Metrics collector for Glue jobs.
    """

    def __init__(self, job_name: str, logger: logging.Logger = None):
        self.job_name = job_name
        self.logger = logger or get_logger(job_name)
        self.metrics = {}
        self.start_time = datetime.utcnow()

    def record_metric(self, name: str, value: Any, unit: str = "Count"):
        """
        Record a metric.

        Args:
            name: Metric name
            value: Metric value
            unit: Unit type
        """
        self.metrics[name] = {
            "value": value,
            "unit": unit,
            "timestamp": datetime.utcnow().isoformat()
        }

        self.logger.info(f"Metric: {name}={value} {unit}")

    def record_row_count(self, stage: str, count: int):
        """Record row count for a stage."""
        self.record_metric(f"rows_{stage}", count, "Count")

    def record_duration(self, stage: str, seconds: float):
        """Record duration for a stage."""
        self.record_metric(f"duration_{stage}", seconds, "Seconds")

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        return {
            "job_name": self.job_name,
            "start_time": self.start_time.isoformat(),
            "end_time": datetime.utcnow().isoformat(),
            "duration_seconds": (datetime.utcnow() - self.start_time).total_seconds(),
            "metrics": self.metrics
        }

    def log_summary(self):
        """Log the metrics summary."""
        self.logger.info(f"Job Summary: {json.dumps(self.get_summary())}")

class StageTimer:
    """
    Context manager for timing job stages.
    """

    def __init__(self, metrics: JobMetrics, stage_name: str):
        self.metrics = metrics
        self.stage_name = stage_name
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.utcnow()
        self.metrics.logger.info(f"Starting stage: {self.stage_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        self.metrics.record_duration(self.stage_name, duration)

        if exc_type:
            self.metrics.logger.error(f"Stage {self.stage_name} failed: {exc_val}")
        else:
            self.metrics.logger.info(f"Completed stage: {self.stage_name} in {duration:.2f}s")

        return False
