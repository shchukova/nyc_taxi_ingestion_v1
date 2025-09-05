# tests/unit/test_tlc_config.py
"""Tests for TLCConfig dataclass."""

import pytest
from src.config.settings import TLCConfig


class TestTLCConfig:
    """Test TLCConfig dataclass functionality."""
    
    def test_tlc_config_creation_with_defaults(self):
        """Test creating TLCConfig with default values."""
        config = TLCConfig()
        
        assert config.base_url == "https://d37ci6vzurychx.cloudfront.net/trip-data"
        assert config.trip_types == ["yellow_tripdata", "green_tripdata"]
        assert config.file_format == "parquet"
        assert config.max_retries == 3
        assert config.timeout_seconds == 300
    
    def test_tlc_config_creation_with_custom_values(self):
        """Test creating TLCConfig with custom values."""
        custom_trip_types = ["yellow_tripdata", "green_tripdata", "fhv_tripdata"]
        config = TLCConfig(
            base_url="https://custom-url.com/data",
            trip_types=custom_trip_types,
            file_format="csv",
            max_retries=5,
            timeout_seconds=600
        )
        
        assert config.base_url == "https://custom-url.com/data"
        assert config.trip_types == custom_trip_types
        assert config.file_format == "csv"
        assert config.max_retries == 5
        assert config.timeout_seconds == 600
    
    def test_tlc_config_post_init_sets_default_trip_types(self):
        """Test that __post_init__ sets default trip_types when None."""
        config = TLCConfig(trip_types=None)
        
        # __post_init__ should set default trip_types
        assert config.trip_types == ["yellow_tripdata", "green_tripdata"]
    
    def test_tlc_config_post_init_preserves_custom_trip_types(self):
        """Test that __post_init__ preserves explicitly set trip_types."""
        custom_types = ["fhv_tripdata"]
        config = TLCConfig(trip_types=custom_types)
        
        # Should preserve the custom trip_types
        assert config.trip_types == custom_types
    
    def test_tlc_config_post_init_with_empty_list(self):
        """Test __post_init__ with empty trip_types list."""
        config = TLCConfig(trip_types=[])
        
        # Should preserve empty list (not None, so no default set)
        assert config.trip_types == []
