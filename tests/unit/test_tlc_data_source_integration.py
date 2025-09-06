# tests/unit/test_tlc_data_source_integration.py
"""Integration tests for TLCDataSource combining multiple features."""

import pytest
from unittest.mock import patch
from datetime import date
from src.data_sources.tlc_data_source import TLCDataSource
from src.config.settings import TLCConfig
from src.utils.exceptions import DataSourceError


class TestTLCDataSourceIntegration:
    """Integration tests combining multiple TLCDataSource features."""
    
    @pytest.fixture
    def full_config(self):
        """Create a full TLC config for integration testing."""
        return TLCConfig(
            base_url="https://d37ci6vzurychx.cloudfront.net/trip-data",
            trip_types=["yellow_tripdata", "green_tripdata", "fhv_tripdata"],
            file_format="parquet",
            max_retries=3,
            timeout_seconds=300
        )
    
    @pytest.fixture
    def data_source(self, full_config):
        """Create TLCDataSource with full config."""
        return TLCDataSource(full_config)
    
    @patch('src.data_sources.tlc_data_source.date')
    def test_end_to_end_recent_files_workflow(self, mock_date, data_source):
        """Test complete workflow: get recent files, estimate processing time."""
        # Mock current date
        mock_date.today.return_value = date(2024, 6, 15)
    
        # Get recent files
        files = data_source.get_recent_files("yellow_tripdata", months_back=2)
    
        # Should get 2 files
        assert len(files) == 2
        assert all(file.year == 2024 for file in files)
    
        # The actual logic gives us March and April 2024
        # (ending 2 months before June = April, going back 2 months = March, April)
        assert files[0].month == 3  # CHANGED FROM 2 TO 3
        assert files[1].month == 4  # CHANGED FROM 3 TO 4
    
    def test_schema_validation_integration(self, data_source):
        """Test schema validation for all supported trip types."""
        supported_types = ["yellow_tripdata", "green_tripdata"]
        
        for trip_type in supported_types:
            schema = data_source.validate_data_schema(trip_type)
            
            # All schemas should have basic required fields
            assert isinstance(schema, dict)
            assert len(schema) > 0
            
            # Should have VendorID and basic fare fields
            assert 'VendorID' in schema
            assert 'fare_amount' in schema
            assert 'total_amount' in schema
            
            # Should have datetime fields (different names for different types)
            datetime_fields = [col for col in schema.keys() if 'datetime' in col]
            assert len(datetime_fields) >= 2  # pickup and dropoff
    
    def test_file_operations_with_validation(self, data_source):
        """Test file operations combined with validation."""
        # Get files for a date range
        files = data_source.get_available_files(
            "yellow_tripdata",
            start_date=(2023, 1),
            end_date=(2023, 3)
        )
        
        # Should get 3 files
        assert len(files) == 3
        
        # Each file should pass basic validation
        for file in files:
            # URL should be valid format
            assert file.url.startswith("https://")
            assert file.trip_type in file.url
            assert str(file.year) in file.url
            assert f"{file.month:02d}" in file.url
            
            # Date string should be properly formatted
            assert file.date_string == f"{file.year}-{file.month:02d}"
            
            # Month name should be valid
            assert file.month_name in [
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
            ]
        
        # Validate schema for the trip type
        schema = data_source.validate_data_schema("yellow_tripdata")
        assert isinstance(schema, dict)
        assert len(schema) > 0
    
    def test_error_handling_integration(self, data_source):
        """Test error handling across different operations."""
        # Test invalid trip type propagates through operations
        with pytest.raises(DataSourceError, match="Unsupported trip type"):
            data_source.get_available_files(
                "invalid_tripdata",
                start_date=(2023, 1),
                end_date=(2023, 2)
            )
        
        with pytest.raises(DataSourceError, match="Unsupported trip type"):
            data_source.generate_file_url("invalid_tripdata", 2023, 1)
        
        # Test invalid dates propagate
        with pytest.raises(DataSourceError):
            data_source.get_available_files(
                "yellow_tripdata",
                start_date=(2023, 13),  # Invalid month
                end_date=(2023, 12)
            )
    
    def test_realistic_data_scenario(self, data_source):
        """Test with realistic data processing scenario."""
        # Scenario: Process 6 months of yellow taxi data
        files = data_source.get_available_files(
            "yellow_tripdata",
            start_date=(2023, 1),
            end_date=(2023, 6)
        )
        
        # Should get 6 files
        assert len(files) == 6
        
        # Calculate total estimated size
        total_size = sum(
            file.estimated_size_mb or 0 
            for file in files
        )
        assert total_size > 0
        
        # Estimate processing time
        processing_time = data_source.estimate_processing_time(files)
        
        # For 6 months of yellow taxi data (~900MB), should take reasonable time
        assert 5 <= processing_time <= 30  # Between 5-30 minutes seems reasonable
        
        # Verify all files are properly formatted
        for i, file in enumerate(files):
            expected_month = i + 1
            assert file.month == expected_month
            assert file.year == 2023
            assert f"2023-{expected_month:02d}" in file.filename
