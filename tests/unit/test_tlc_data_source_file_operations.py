# tests/unit/test_tlc_data_source_file_operations.py
"""Tests for TLCDataSource file operations."""

import pytest
from unittest.mock import patch
from datetime import date
from src.data_sources.tlc_data_source import TLCDataSource, TLCDataFile
from src.config.settings import TLCConfig
from src.utils.exceptions import DataSourceError


class TestTLCDataSourceFileOperations:
    """Test file-related operations in TLCDataSource."""
    
    @pytest.fixture
    def data_source(self):
        """Create TLCDataSource for testing."""
        config = TLCConfig(
            trip_types=["yellow_tripdata", "green_tripdata"],
            file_format="parquet"
        )
        return TLCDataSource(config)
    
    def test_get_available_files_single_month(self, data_source):
        """Test getting available files for a single month."""
        files = data_source.get_available_files(
            "yellow_tripdata",
            start_date=(2023, 6),
            end_date=(2023, 6)
        )
        
        assert len(files) == 1
        file = files[0]
        assert isinstance(file, TLCDataFile)
        assert file.trip_type == "yellow_tripdata"
        assert file.year == 2023
        assert file.month == 6
        assert "yellow_tripdata_2023-06.parquet" in file.url
        assert file.filename == "yellow_tripdata_2023-06.parquet"
    
    def test_get_available_files_multiple_months_same_year(self, data_source):
        """Test getting available files for multiple months in same year."""
        files = data_source.get_available_files(
            "green_tripdata",
            start_date=(2023, 3),
            end_date=(2023, 5)
        )
        
        assert len(files) == 3
        
        # Check files are in chronological order
        expected_months = [3, 4, 5]
        for i, expected_month in enumerate(expected_months):
            assert files[i].year == 2023
            assert files[i].month == expected_month
            assert files[i].trip_type == "green_tripdata"
    
    def test_get_available_files_across_year_boundary(self, data_source):
        """Test getting files across year boundary."""
        files = data_source.get_available_files(
            "yellow_tripdata",
            start_date=(2023, 11),
            end_date=(2024, 2)
        )
        
        assert len(files) == 4  # Nov, Dec 2023, Jan, Feb 2024
        
        expected_dates = [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]
        for i, (expected_year, expected_month) in enumerate(expected_dates):
            assert files[i].year == expected_year
            assert files[i].month == expected_month
    
    def test_get_available_files_invalid_trip_type(self, data_source):
        """Test get_available_files with invalid trip type."""
        with pytest.raises(DataSourceError, match="Unsupported trip type"):
            data_source.get_available_files(
                "invalid_tripdata",
                start_date=(2023, 1),
                end_date=(2023, 2)
            )
    
    def test_get_available_files_invalid_date_range(self, data_source):
        """Test get_available_files with invalid date range."""
        # Start date after end date
        with pytest.raises(DataSourceError, match="Start date cannot be after end date"):
            data_source.get_available_files(
                "yellow_tripdata",
                start_date=(2023, 6),
                end_date=(2023, 3)
            )
    
    def test_get_available_files_invalid_dates(self, data_source):
        """Test get_available_files with invalid dates."""
        # Invalid month
        with pytest.raises(DataSourceError, match="Invalid date range"):
            data_source.get_available_files(
                "yellow_tripdata",
                start_date=(2023, 13),  # Invalid month
                end_date=(2023, 12)
            )
    
    def test_get_available_files_includes_estimated_size(self, data_source):
        """Test that files include estimated size from known sizes."""
        files = data_source.get_available_files(
            "yellow_tripdata",
            start_date=(2023, 1),
            end_date=(2023, 1)
        )
        
        assert len(files) == 1
        file = files[0]
        assert file.estimated_size_mb is not None
        assert file.estimated_size_mb > 0
    
    @patch('src.data_sources.tlc_data_source.date')
    def test_get_recent_files(self, mock_date, data_source):
        """Test getting recent files with mocked current date."""
        # Mock current date as May 2024
        mock_date.today.return_value = date(2024, 5, 15)
        
        # Should get files for 3 months ending 2 months ago (Jan, Feb, Mar 2024)
        files = data_source.get_recent_files("yellow_tripdata", months_back=3)
        
        assert len(files) == 3
        
        # Should be January, February, March 2024 (ending 2 months before May)
        expected_months = [1, 2, 3]
        for i, expected_month in enumerate(expected_months):
            assert files[i].year == 2024
            assert files[i].month == expected_month
    
    @patch('src.data_sources.tlc_data_source.date')
    def test_get_recent_files_across_year_boundary(self, mock_date, data_source):
        """Test get_recent_files when it crosses year boundary."""
        # Mock current date as March 2024
        mock_date.today.return_value = date(2024, 3, 15)
        
        # Should get files for 3 months ending 2 months ago (Nov, Dec 2023, Jan 2024)
        files = data_source.get_recent_files("yellow_tripdata", months_back=3)
        
        assert len(files) == 3
        
        expected_dates = [(2023, 11), (2023, 12), (2024, 1)]
        for i, (expected_year, expected_month) in enumerate(expected_dates):
            assert files[i].year == expected_year
            assert files[i].month == expected_month
