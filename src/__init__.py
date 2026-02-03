"""
NYC Taxi Pipeline
=================

A data pipeline for processing NYC Taxi data using PySpark,
loading into a dimensional model, and deploying to GCP.

Modules:
    - jobs: PySpark jobs for data processing
    - models: Data schemas and dimensional model definitions
    - utils: Utility functions and configuration
    - quality: Data quality validators
"""

__version__ = "0.1.0"
__author__ = "Marcelo"

from src.utils.config import PipelineConfig, Environment

__all__ = [
    "__version__",
    "PipelineConfig",
    "Environment",
]
