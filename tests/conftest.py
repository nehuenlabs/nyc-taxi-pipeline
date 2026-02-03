"""
Pytest Configuration and Fixtures
==================================

Shared fixtures and configuration for all tests.
"""

import os
import sys
import pytest

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment variables."""
    # Ensure we're in local/test mode
    os.environ["PIPELINE_ENV"] = "local"
    os.environ["DATA_PATH"] = "/tmp/test_data"
    
    yield
    
    # Cleanup if needed


@pytest.fixture
def sample_trip_record():
    """Sample taxi trip record for testing."""
    return {
        "VendorID": 1,
        "tpep_pickup_datetime": "2023-01-15 10:30:00",
        "tpep_dropoff_datetime": "2023-01-15 10:45:00",
        "passenger_count": 2,
        "trip_distance": 3.5,
        "PULocationID": 161,
        "DOLocationID": 237,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "payment_type": 1,
        "fare_amount": 15.50,
        "extra": 1.00,
        "mta_tax": 0.50,
        "tip_amount": 3.00,
        "tolls_amount": 0.00,
        "improvement_surcharge": 0.30,
        "total_amount": 20.30,
        "congestion_surcharge": 2.50,
        "Airport_fee": 0.00,
    }


@pytest.fixture
def sample_zone_record():
    """Sample zone lookup record for testing."""
    return {
        "LocationID": 161,
        "Borough": "Manhattan",
        "Zone": "Midtown Center",
        "service_zone": "Yellow Zone",
    }
