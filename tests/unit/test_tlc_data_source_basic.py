# tests/unit/test_tlc_data_source_basic.py
"""Basic tests for TLCDataSource class."""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from src.data_sources.tlc_data_source import TLCDataSource, TLCDataFile
from src.config.settings import TLCConfig
from src.utils.exceptions import DataSourceError


class TestTLCDataSourceBasic:
    """Test basic TLCDataSource functionality."""
    
    @pytest.fixture
    def tlc_config(self):
        """Create a mock TLC config for testing."""
        config = TLCConfig(
            base_url="https://d37ci6vzurychx.cloudfront.net/trip-data",
            trip_types=["yellow_tripdata", "green_tripdata"],
            file_format="parquet",
            max_retries=3,
            timeout_seconds=300
        )
        return config
    
    @pytest.fixture
    def data_source(self, tlc_config):
        """Create TLCDataSource instance for testing."""
        return TLCDataSource(tlc_config)
    
    def test_initialization(self, tlc_config):
        """Test TLCDataSource initialization."""
        data_source = TLCDataSource(tlc_config)
        
        assert data_source.config == tlc_config
        assert hasattr(data_source, '_known_file_sizes')
        assert isinstance(data_source._known_file_sizes, dict)
    
    def test_initialize_file_size_estimates(self, data_source):
        """Test file size estimates are initialized."""
        estimates = data_source._known_file_sizes
        
        # Check that common trip types have estimates
        assert "yellow_tripdata" in estimates
        assert "green_tripdata" in estimates
        assert "fhv_tripdata" in estimates
        assert "fhvhv_tripdata" in estimates
        
        # Check estimates are reasonable (in MB)
        assert estimates["yellow_tripdata"] > 0
        assert estimates["green_tripdata"] > 0
        assert estimates["fhv_tripdata"] > 0
        assert estimates["fhvhv_tripdata"] > 0
    
    def test_generate_file_url_valid_inputs(self, data_source):
        """Test URL generation with valid inputs."""
        url = data_source.generate_file_url("yellow_tripdata", 2024, 3)
        
        expected_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet"
        assert url == expected_url
    
    def test_generate_file_url_different_trip_types(self, data_source):
        """Test URL generation for different trip types."""
        test_cases = [
            ("yellow_tripdata", "yellow_tripdata_2024-01.parquet"),
            ("green_tripdata", "green_tripdata_2024-01.parquet")
        ]
        
        for trip_type, expected_filename in test_cases:
            url = data_source.generate_file_url(trip_type, 2024, 1)
            assert expected_filename in url
    
    def test_generate_file_url_different_dates(self, data_source):
        """Test URL generation for different dates."""
        test_cases = [
            (2024, 1, "2024-01"),
            (2024, 10, "2024-10"),
            (2023, 6, "2023-06")
        ]
        
        for year, month, expected_date in test_cases:
            url = data_source.generate_file_url("yellow_tripdata", year, month)
            assert expected_date in url
    
    def test_generate_file_url_unsupported_trip_type(self, data_source):
        """Test URL generation with unsupported trip type."""
        with pytest.raises(DataSourceError, match="Unsupported trip type"):
            data_source.generate_file_url("invalid_tripdata", 2024, 1)
    
    def test_generate_file_url_invalid_date(self, data_source):
        """Test URL generation with invalid dates."""
        # Test invalid month
        with pytest.raises(DataSourceError, match="Invalid date"):
            data_source.generate_file_url("yellow_tripdata", 2024, 13)
        
        # Test invalid month (0)
        with pytest.raises(DataSourceError, match="Invalid date"):
            data_source.generate_file_url("yellow_tripdata", 2024, 0)
        
        # Test year too early
        with pytest.raises(DataSourceError, match="Invalid date"):
            data_source.generate_file_url("yellow_tripdata", 2008, 1)