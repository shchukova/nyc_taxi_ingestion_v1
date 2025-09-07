# tests/unit/conftest.py (additional fixtures for snowflake loader tests)
"""
Additional pytest fixtures for SnowflakeLoader tests
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock

from src.config.settings import SnowflakeConfig
from src.data_sources.tlc_data_source import TLCDataFile


@pytest.fixture
def snowflake_config_minimal():
    """Create minimal Snowflake configuration for testing"""
    return SnowflakeConfig(
        account="test",
        username="test",
        password="test",
        warehouse="test",
        database="test",
        schema="test"
    )


@pytest.fixture
def mock_snowflake_connection():
    """Create a mock Snowflake connection"""
    connection = Mock()
    cursor = Mock()
    connection.cursor.return_value = cursor
    
    # Setup default cursor behaviors
    cursor.execute.return_value = None
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    cursor.description = []
    cursor.close.return_value = None
    connection.close.return_value = None
    
    return connection


@pytest.fixture
def large_sample_dataframe():
    """Create a larger sample DataFrame for batch testing"""
    import numpy as np
    
    size = 1000
    return pd.DataFrame({
        'VendorID': np.random.choice([1, 2], size),
        'tpep_pickup_datetime': pd.date_range('2024-01-01', periods=size, freq='1H'),
        'tpep_dropoff_datetime': pd.date_range('2024-01-01 00:30:00', periods=size, freq='1H'),
        'passenger_count': np.random.uniform(1, 4, size),
        'trip_distance': np.random.uniform(0.5, 50, size),
        'total_amount': np.random.uniform(5, 200, size),
        'fare_amount': np.random.uniform(3, 150, size),
        'tip_amount': np.random.uniform(0, 30, size)
    })


@pytest.fixture
def green_taxi_dataframe():
    """Create a sample DataFrame with green taxi data structure"""
    return pd.DataFrame({
        'VendorID': [1, 2, 1, 2],
        'lpep_pickup_datetime': pd.to_datetime([
            '2024-01-01 10:00:00', '2024-01-01 11:00:00',
            '2024-01-01 12:00:00', '2024-01-01 13:00:00'
        ]),
        'lpep_dropoff_datetime': pd.to_datetime([
            '2024-01-01 10:30:00', '2024-01-01 11:45:00',
            '2024-01-01 12:20:00', '2024-01-01 13:30:00'
        ]),
        'passenger_count': [1.0, 2.0, 1.0, 3.0],
        'trip_distance': [2.5, 8.1, 1.2, 5.4],
        'total_amount': [15.5, 35.2, 8.1, 28.7],
        'trip_type': [1, 1, 2, 1],
        'ehail_fee': [0.0, 0.0, 2.5, 0.0]
    })


@pytest.fixture
def green_taxi_data_file():
    """Create sample TLCDataFile for green taxi data"""
    return TLCDataFile(
        trip_type="green_tripdata",
        year=2024,
        month=2,
        url="https://example.com/green_test.parquet",
        filename="green_tripdata_2024-02.parquet",
        estimated_size_mb=75
    )


@pytest.fixture
def temp_parquet_file(sample_dataframe):
    """Create a temporary parquet file with sample data"""
    temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    temp_path = Path(temp_file.name)
    temp_file.close()
    
    # Write sample data to the file
    sample_dataframe.to_parquet(temp_path, index=False)
    
    yield temp_path
    
    # Cleanup
    if temp_path.exists():
        os.unlink(temp_path)


@pytest.fixture
def corrupted_dataframe():
    """Create a DataFrame with data quality issues for testing validation"""
    return pd.DataFrame({
        'VendorID': [1, 2, None, 1, 2],
        'tpep_pickup_datetime': [
            None, None, None,  # High null percentage
            '2024-01-01 12:00:00', '2024-01-01 10:00:00'
        ],
        'tpep_dropoff_datetime': [
            '2024-01-01 10:30:00', '2024-01-01 11:30:00', 
            '2024-01-01 12:30:00', '2024-01-01 11:30:00',  # This one has pickup after dropoff
            '2024-01-01 10:30:00'
        ],
        'passenger_count': [1.0, 2.0, 1.0, -1.0, 10.0],  # Negative and extreme values
        'trip_distance': [2.5, 3.2, 1.8, -5.0, 1000.0],  # Negative and extreme values
        'total_amount': [-15.5, 22.3, 12.1, 2000.0, 5.0]  # Negative and extreme values
    })


@pytest.fixture(scope="session")
def test_database_config():
    """Configuration for test database operations"""
    return {
        'test_schema': 'test_schema',
        'test_table_prefix': 'test_',
        'cleanup_tables': True
    }

@pytest.fixture
def loader(snowflake_config_minimal):
    """
    Create a SnowflakeLoader instance for testing.
    Uses the existing snowflake_config_minimal fixture.
    """
    from src.loaders.snowflake_loader import SnowflakeLoader
    
    # Create loader with minimal config
    return SnowflakeLoader(snowflake_config_minimal)


@pytest.fixture
def sample_data_file(temp_parquet_file):
    """
    Provide a sample data file path for testing.
    Uses the existing temp_parquet_file fixture.
    """
    return str(temp_parquet_file)

@pytest.fixture
def snowflake_config_minimal():
    """Create minimal Snowflake configuration for testing"""
    return SnowflakeConfig(
        account="test",
        username="test",
        password="test",
        warehouse="test",
        database="test",
        schema="test"
    )


@pytest.fixture
def loader(snowflake_config_minimal):
    """
    Create a SnowflakeLoader instance for testing.
    Uses the existing snowflake_config_minimal fixture.
    """
    from src.loaders.snowflake_loader import SnowflakeLoader
    
    # Create loader with minimal config
    return SnowflakeLoader(snowflake_config_minimal)


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing (if this doesn't exist)"""
    return pd.DataFrame({
        'VendorID': [1, 2, 1],
        'tpep_pickup_datetime': pd.to_datetime(['2024-01-01 10:00:00', '2024-01-01 11:00:00', '2024-01-01 12:00:00']),
        'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01 10:30:00', '2024-01-01 11:30:00', '2024-01-01 12:30:00']),
        'passenger_count': [1, 2, 1],
        'trip_distance': [2.5, 3.2, 1.8],
        'total_amount': [15.5, 22.3, 12.1]
    })


@pytest.fixture
def sample_data_file(temp_parquet_file):
    """
    Provide a sample data file path for testing.
    Uses the existing temp_parquet_file fixture.
    """
    return str(temp_parquet_file)


@pytest.fixture
def mock_snowflake_connection():
    """Create a mock Snowflake connection"""
    connection = Mock()
    cursor = Mock()
    connection.cursor.return_value = cursor
    
    # Setup default cursor behaviors
    cursor.execute.return_value = None
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    cursor.description = []
    cursor.close.return_value = None
    connection.close.return_value = None
    
    return connection


@pytest.fixture
def large_sample_dataframe():
    """Create a larger sample DataFrame for batch testing"""
    import numpy as np
    
    size = 1000
    return pd.DataFrame({
        'VendorID': np.random.choice([1, 2], size),
        'tpep_pickup_datetime': pd.date_range('2024-01-01', periods=size, freq='1H'),
        'tpep_dropoff_datetime': pd.date_range('2024-01-01 00:30:00', periods=size, freq='1H'),
        'passenger_count': np.random.uniform(1, 4, size),
        'trip_distance': np.random.uniform(0.5, 50, size),
        'total_amount': np.random.uniform(5, 200, size),
        'fare_amount': np.random.uniform(3, 150, size),
        'tip_amount': np.random.uniform(0, 30, size)
    })


@pytest.fixture
def green_taxi_dataframe():
    """Create a sample DataFrame with green taxi data structure"""
    return pd.DataFrame({
        'VendorID': [1, 2, 1, 2],
        'lpep_pickup_datetime': pd.to_datetime([
            '2024-01-01 10:00:00', '2024-01-01 11:00:00',
            '2024-01-01 12:00:00', '2024-01-01 13:00:00'
        ]),
        'lpep_dropoff_datetime': pd.to_datetime([
            '2024-01-01 10:30:00', '2024-01-01 11:45:00',
            '2024-01-01 12:20:00', '2024-01-01 13:30:00'
        ]),
        'passenger_count': [1.0, 2.0, 1.0, 3.0],
        'trip_distance': [2.5, 8.1, 1.2, 5.4],
        'total_amount': [15.5, 35.2, 8.1, 28.7],
        'trip_type': [1, 1, 2, 1],
        'ehail_fee': [0.0, 0.0, 2.5, 0.0]
    })


@pytest.fixture
def green_taxi_data_file():
    """Create sample TLCDataFile for green taxi data"""
    return TLCDataFile(
        trip_type="green_tripdata",
        year=2024,
        month=2,
        url="https://example.com/green_test.parquet",
        filename="green_tripdata_2024-02.parquet",
        estimated_size_mb=75
    )


@pytest.fixture
def temp_parquet_file(sample_dataframe):
    """Create a temporary parquet file with sample data"""
    temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    temp_path = Path(temp_file.name)
    temp_file.close()
    
    # Write sample data to the file
    sample_dataframe.to_parquet(temp_path, index=False)
    
    yield temp_path
    
    # Cleanup
    if temp_path.exists():
        os.unlink(temp_path)


@pytest.fixture
def corrupted_dataframe():
    """Create a DataFrame with data quality issues for testing validation"""
    return pd.DataFrame({
        'VendorID': [1, 2, None, 1, 2],
        'tpep_pickup_datetime': [
            None, None, None,  # High null percentage
            '2024-01-01 12:00:00', '2024-01-01 10:00:00'
        ],
        'tpep_dropoff_datetime': [
            '2024-01-01 10:30:00', '2024-01-01 11:30:00', 
            '2024-01-01 12:30:00', '2024-01-01 11:30:00',  # This one has pickup after dropoff
            '2024-01-01 10:30:00'
        ],
        'passenger_count': [1.0, 2.0, 1.0, -1.0, 10.0],  # Negative and extreme values
        'trip_distance': [2.5, 3.2, 1.8, -5.0, 1000.0],  # Negative and extreme values
        'total_amount': [-15.5, 22.3, 12.1, 2000.0, 5.0]  # Negative and extreme values
    })


@pytest.fixture(scope="session")
def test_database_config():
    """Configuration for test database operations"""
    return {
        'test_schema': 'test_schema',
        'test_table_prefix': 'test_',
        'cleanup_tables': True
    }

@pytest.fixture
def snowflake_config():
    """Create Snowflake configuration for testing"""
    return SnowflakeConfig(
        account="test_account",      # Match what test expects
        username="test_user",        # Match what test expects
        password="test_password",
        warehouse="test_warehouse",  # Match what test expects
        database="test_database",    # Match what test expects
        schema="test_schema"         # Match what test expects
    )