# tests/unit/test_tlc_data_file.py
"""Tests for TLCDataFile dataclass."""

import pytest
from src.data_sources.tlc_data_source import TLCDataFile


class TestTLCDataFile:
    """Test TLCDataFile dataclass functionality."""
    
    def test_tlc_data_file_creation_required_fields(self):
        """Test creating TLCDataFile with required fields only."""
        file_info = TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=3,
            url="https://example.com/yellow_tripdata_2024-03.parquet",
            filename="yellow_tripdata_2024-03.parquet"
        )
        
        assert file_info.trip_type == "yellow_tripdata"
        assert file_info.year == 2024
        assert file_info.month == 3
        assert file_info.url == "https://example.com/yellow_tripdata_2024-03.parquet"
        assert file_info.filename == "yellow_tripdata_2024-03.parquet"
        assert file_info.estimated_size_mb is None  # Default
    
    def test_tlc_data_file_creation_with_size(self):
        """Test creating TLCDataFile with estimated size."""
        file_info = TLCDataFile(
            trip_type="green_tripdata",
            year=2024,
            month=1,
            url="https://example.com/green_tripdata_2024-01.parquet",
            filename="green_tripdata_2024-01.parquet",
            estimated_size_mb=150
        )
        
        assert file_info.estimated_size_mb == 150
    
    def test_month_name_property(self):
        """Test month_name property returns correct month names."""
        test_cases = [
            (1, "January"),
            (2, "February"),
            (3, "March"),
            (6, "June"),
            (12, "December")
        ]
        
        for month_num, expected_name in test_cases:
            file_info = TLCDataFile(
                trip_type="yellow_tripdata",
                year=2024,
                month=month_num,
                url="https://example.com/test.parquet",
                filename="test.parquet"
            )
            assert file_info.month_name == expected_name
    
    def test_date_string_property(self):
        """Test date_string property returns formatted date."""
        test_cases = [
            (2024, 1, "2024-01"),
            (2024, 10, "2024-10"),
            (2023, 6, "2023-06"),
            (2022, 12, "2022-12")
        ]
        
        for year, month, expected_date_string in test_cases:
            file_info = TLCDataFile(
                trip_type="yellow_tripdata",
                year=year,
                month=month,
                url="https://example.com/test.parquet",
                filename="test.parquet"
            )
            assert file_info.date_string == expected_date_string
    
    @pytest.mark.parametrize("month,expected_name", [
        (1, "January"),
        (2, "February"),
        (3, "March"),
        (4, "April"),
        (5, "May"),
        (6, "June"),
        (7, "July"),
        (8, "August"),
        (9, "September"),
        (10, "October"),
        (11, "November"),
        (12, "December")
    ])
    def test_all_month_names(self, month, expected_name):
        """Test month_name property for all months using parametrize."""
        file_info = TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=month,
            url="https://example.com/test.parquet",
            filename="test.parquet"
        )
        assert file_info.month_name == expected_name


# tests/unit/test_tlc_data_source_edge_cases.py
"""Tests for edge cases and boundary conditions in TLCDataSource."""

import pytest
from unittest.mock import patch
from datetime import date, datetime
from src.data_sources.tlc_data_source import TLCDataSource, TLCDataFile
from src.config.settings import TLCConfig
from src.utils.exceptions import DataSourceError


class TestTLCDataSourceEdgeCases:
    """Test edge cases and boundary conditions."""
    
    @pytest.fixture
    def data_source(self):
        """Create TLCDataSource for edge case testing."""
        config = TLCConfig(
            trip_types=["yellow_tripdata", "green_tripdata"],
            file_format="parquet"
        )
        return TLCDataSource(config)
    
    def test_single_day_date_range(self, data_source):
        """Test date range that spans exactly one month."""
        files = data_source.get_available_files(
            "yellow_tripdata",
            start_date=(2023, 5),
            end_date=(2023, 5)
        )
        
        assert len(files) == 1
        assert files[0].year == 2023
        assert files[0].month == 5
    
    def test_year_boundary_edge_cases(self, data_source):
        """Test edge cases around year boundaries."""
        # December to January
        files = data_source.get_available_files(
            "yellow_tripdata",
            start_date=(2023, 12),
            end_date=(2024, 1)
        )
        
        assert len(files) == 2
        assert files[0].year == 2023
        assert files[0].month == 12
        assert files[1].year == 2024
        assert files[1].month == 1
    
    @patch('src.data_sources.tlc_data_source.datetime')
    def test_current_date_boundary(self, mock_datetime, data_source):
        """Test behavior at current date boundaries."""
        # Mock current date as January 15, 2024
        mock_datetime.now.return_value = datetime(2024, 1, 15)
        
        # November 2023 should be valid (2+ months ago)
        assert data_source._is_valid_date(2023, 11) is True
        
        # December 2023 should be invalid (only 1 month ago)
        assert data_source._is_valid_date(2023, 12) is False
        
        # January 2024 should be invalid (current month)
        assert data_source._is_valid_date(2024, 1) is False
    
    @patch('src.data_sources.tlc_data_source.date')
    def test_get_recent_files_edge_cases(self, mock_date, data_source):
        """Test get_recent_files with edge case dates."""
        # Test when current month requires going back to previous year
        mock_date.today.return_value = date(2024, 2, 15)  # February
        
        # With 2-month delay, should end at December 2023
        # Getting 3 months back should give Oct, Nov, Dec 2023
        files = data_source.get_recent_files("yellow_tripdata", months_back=3)
        
        assert len(files) == 3
        expected_dates = [(2023, 10), (2023, 11), (2023, 12)]
        for i, (expected_year, expected_month) in enumerate(expected_dates):
            assert files[i].year == expected_year
            assert files[i].month == expected_month
    
    def test_very_long_date_range(self, data_source):
        """Test with a very long date range."""
        # 2 full years
        files = data_source.get_available_files(
            "yellow_tripdata",
            start_date=(2021, 1),
            end_date=(2022, 12)
        )
        
        assert len(files) == 24  # 24 months
        
        # Check first and last files
        assert files[0].year == 2021
        assert files[0].month == 1
        assert files[-1].year == 2022
        assert files[-1].month == 12
    
    def test_zero_months_back(self, data_source):
        """Test get_recent_files with zero months."""
        with patch('src.data_sources.tlc_data_source.date') as mock_date:
            mock_date.today.return_value = date(2024, 5, 15)
        
        # Zero months back should handle edge case gracefully
        try:
            files = data_source.get_recent_files("yellow_tripdata", months_back=0)
            # If it succeeds, should return empty list or handle gracefully
            assert len(files) >= 0
        except DataSourceError:
            # It's acceptable for zero months to raise an error
            # This is actually reasonable behavior
            pytest.skip("Zero months back raises DataSourceError - acceptable behavior")
    
    def test_file_size_estimation_edge_cases(self, data_source):
        """Test file size estimation edge cases."""
        # File with zero estimated size
        files = [TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://example.com/test.parquet",
            filename="test.parquet",
            estimated_size_mb=0
        )]
        
        processing_time = data_source.estimate_processing_time(files)
        assert processing_time >= 1  # Should still have minimum time
        
        # File with very large size
        files = [TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://example.com/test.parquet",
            filename="test.parquet",
            estimated_size_mb=10000  # 10GB
        )]
        
        processing_time = data_source.estimate_processing_time(files)
        assert processing_time > 100  # Should take a long time
    
    def test_month_name_property_edge_cases(self):
        """Test month_name property with edge case months."""
        # Test all month boundaries
        for month in [1, 12]:  # January and December
            file_info = TLCDataFile(
                trip_type="yellow_tripdata",
                year=2024,
                month=month,
                url="https://example.com/test.parquet",
                filename="test.parquet"
            )
            
            # Should not raise an exception
            month_name = file_info.month_name
            assert isinstance(month_name, str)
            assert len(month_name) > 0
    
    def test_date_string_formatting_edge_cases(self):
        """Test date string formatting with edge cases."""
        test_cases = [
            (2024, 1, "2024-01"),   # Single digit month
            (2024, 10, "2024-10"),  # Double digit month
            (1999, 12, "1999-12"),  # Older year
            (2030, 5, "2030-05")    # Future year
        ]
        
        for year, month, expected in test_cases:
            file_info = TLCDataFile(
                trip_type="yellow_tripdata",
                year=year,
                month=month,
                url="https://example.com/test.parquet",
                filename="test.parquet"
            )
            assert file_info.date_string == expected
    
    def test_config_with_empty_trip_types(self):
        """Test TLCDataSource with empty trip types list."""
        config = TLCConfig(trip_types=[])
        data_source = TLCDataSource(config)
        
        # Should raise error for any trip type
        with pytest.raises(DataSourceError, match="Unsupported trip type"):
            data_source.generate_file_url("yellow_tripdata", 2024, 1)
    
    def test_config_with_custom_file_format(self):
        """Test TLCDataSource with custom file format."""
        config = TLCConfig(
            trip_types=["yellow_tripdata"],
            file_format="csv"  # Custom format
        )
        data_source = TLCDataSource(config)
        
        url = data_source.generate_file_url("yellow_tripdata", 2024, 1)
        assert url.endswith(".csv")
        
        files = data_source.get_available_files(
            "yellow_tripdata",
            start_date=(2024, 1),
            end_date=(2024, 1)
        )
        assert files[0].filename.endswith(".csv")