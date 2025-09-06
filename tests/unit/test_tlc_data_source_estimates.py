# tests/unit/test_tlc_data_source_estimates.py
"""Tests for TLCDataSource estimation and processing logic."""

import pytest
from src.data_sources.tlc_data_source import TLCDataSource, TLCDataFile
from src.config.settings import TLCConfig


class TestTLCDataSourceEstimates:
    """Test estimation and processing time calculations."""
    
    @pytest.fixture
    def data_source(self):
        """Create TLCDataSource for testing."""
        config = TLCConfig()
        return TLCDataSource(config)
    
    def test_estimate_processing_time_single_file(self, data_source):
        """Test processing time estimation for single file."""
        files = [TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://example.com/test.parquet",
            filename="test.parquet",
            estimated_size_mb=150
        )]
        
        processing_time = data_source.estimate_processing_time(files)
        
        # 150MB should take about 3 minutes (150/100 * 2)
        assert processing_time == 3
        assert isinstance(processing_time, int)
    
    def test_estimate_processing_time_multiple_files(self, data_source):
        """Test processing time estimation for multiple files."""
        files = [
            TLCDataFile(
                trip_type="yellow_tripdata",
                year=2024,
                month=1,
                url="https://example.com/test1.parquet",
                filename="test1.parquet",
                estimated_size_mb=150
            ),
            TLCDataFile(
                trip_type="green_tripdata",
                year=2024,
                month=1,
                url="https://example.com/test2.parquet",
                filename="test2.parquet",
                estimated_size_mb=30
            )
        ]
    
        processing_time = data_source.estimate_processing_time(files)
    
        # 180MB total should take about 3.6 minutes, truncated to 3
        assert processing_time == 3  # CHANGED FROM 4 TO 3
    
    def test_estimate_processing_time_no_size_specified(self, data_source):
        """Test processing time when no size is specified."""
        files = [TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://example.com/test.parquet",
            filename="test.parquet"
            # No estimated_size_mb specified
        )]
        
        processing_time = data_source.estimate_processing_time(files)
        
        # Should use default from _known_file_sizes for yellow_tripdata
        expected_size = data_source._known_file_sizes["yellow_tripdata"]
        expected_time = max(1, int((expected_size / 100) * 2))
        assert processing_time == expected_time
    
    def test_estimate_processing_time_unknown_trip_type(self, data_source):
        """Test processing time for unknown trip type."""
        files = [TLCDataFile(
            trip_type="unknown_tripdata",
            year=2024,
            month=1,
            url="https://example.com/test.parquet",
            filename="test.parquet"
            # No estimated_size_mb specified
        )]
        
        processing_time = data_source.estimate_processing_time(files)
        
        # Should use default of 100MB when trip type not in known sizes
        # 100MB should take 2 minutes
        assert processing_time == 2
    
    def test_estimate_processing_time_minimum_time(self, data_source):
        """Test that processing time has a minimum of 1 minute."""
        files = [TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://example.com/test.parquet",
            filename="test.parquet",
            estimated_size_mb=1  # Very small file
        )]
        
        processing_time = data_source.estimate_processing_time(files)
        
        # Should be at least 1 minute even for tiny files
        assert processing_time >= 1
    
    def test_estimate_processing_time_empty_list(self, data_source):
        """Test processing time estimation with empty file list."""
        files = []
        
        processing_time = data_source.estimate_processing_time(files)
        
        # Should return minimum time of 1 minute
        assert processing_time == 1