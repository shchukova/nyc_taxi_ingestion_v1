# tests/unit/test_tlc_data_source_schema_validation.py
"""Tests for TLCDataSource schema validation."""

import pytest
from src.data_sources.tlc_data_source import TLCDataSource
from src.config.settings import TLCConfig
from src.utils.exceptions import DataSourceError


class TestTLCDataSourceSchemaValidation:
    """Test schema validation functionality."""
    
    @pytest.fixture
    def data_source(self):
        """Create TLCDataSource for testing."""
        config = TLCConfig()
        return TLCDataSource(config)
    
    def test_validate_data_schema_yellow_tripdata(self, data_source):
        """Test schema validation for yellow taxi data."""
        schema = data_source.validate_data_schema("yellow_tripdata")
        
        # Check that key columns are present
        required_columns = [
            'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
            'passenger_count', 'trip_distance', 'fare_amount', 'total_amount'
        ]
        
        for column in required_columns:
            assert column in schema
        
        # Check data types are specified
        assert schema['VendorID'] == 'int64'
        assert schema['tpep_pickup_datetime'] == 'datetime64[ns]'
        assert schema['tpep_dropoff_datetime'] == 'datetime64[ns]'
        assert schema['trip_distance'] == 'float64'
        assert schema['fare_amount'] == 'float64'
    
    def test_validate_data_schema_green_tripdata(self, data_source):
        """Test schema validation for green taxi data."""
        schema = data_source.validate_data_schema("green_tripdata")
        
        # Check that key columns are present (note different datetime columns)
        required_columns = [
            'VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime',
            'passenger_count', 'trip_distance', 'fare_amount', 'total_amount'
        ]
        
        for column in required_columns:
            assert column in schema
        
        # Check green-specific columns
        assert 'lpep_pickup_datetime' in schema  # Green uses 'lpep_' prefix
        assert 'lpep_dropoff_datetime' in schema
        assert 'trip_type' in schema  # Green-specific field
        assert 'ehail_fee' in schema  # Green-specific field
        
        # Check data types
        assert schema['lpep_pickup_datetime'] == 'datetime64[ns]'
        assert schema['trip_type'] == 'int64'
    
    def test_validate_data_schema_unsupported_trip_type(self, data_source):
        """Test schema validation with unsupported trip type."""
        with pytest.raises(DataSourceError, match="Schema not defined for trip type"):
            data_source.validate_data_schema("unsupported_tripdata")
    
    def test_validate_data_schema_yellow_vs_green_differences(self, data_source):
        """Test differences between yellow and green schemas."""
        yellow_schema = data_source.validate_data_schema("yellow_tripdata")
        green_schema = data_source.validate_data_schema("green_tripdata")
        
        # Yellow has 'tpep_' prefix, green has 'lpep_' prefix
        assert 'tpep_pickup_datetime' in yellow_schema
        assert 'tpep_pickup_datetime' not in green_schema
        assert 'lpep_pickup_datetime' in green_schema
        assert 'lpep_pickup_datetime' not in yellow_schema
        
        # Green has additional fields
        assert 'trip_type' in green_schema
        assert 'trip_type' not in yellow_schema
        assert 'ehail_fee' in green_schema
        assert 'ehail_fee' not in yellow_schema
