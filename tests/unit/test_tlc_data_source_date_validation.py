# tests/unit/test_tlc_data_source_date_validation.py
"""Tests for TLCDataSource date validation logic."""

import pytest
from unittest.mock import patch
from datetime import datetime
from src.data_sources.tlc_data_source import TLCDataSource
from src.config.settings import TLCConfig


class TestTLCDataSourceDateValidation:
    """Test date validation logic in TLCDataSource."""
    
    @pytest.fixture
    def data_source(self):
        """Create TLCDataSource with minimal config."""
        config = TLCConfig(trip_types=["yellow_tripdata"])
        return TLCDataSource(config)
    
    def test_is_valid_date_valid_months(self, data_source):
        """Test _is_valid_date with valid months."""
        for month in range(1, 13):
            assert data_source._is_valid_date(2023, month) is True
    
    def test_is_valid_date_invalid_months(self, data_source):
        """Test _is_valid_date with invalid months."""
        invalid_months = [0, 13, -1, 15]
        for month in invalid_months:
            assert data_source._is_valid_date(2023, month) is False
    
    def test_is_valid_date_valid_years(self, data_source):
        """Test _is_valid_date with valid years."""
        valid_years = [2009, 2010, 2020, 2023]
        for year in valid_years:
            # Use a month that's definitely in the past
            assert data_source._is_valid_date(year, 1) is True
    
    def test_is_valid_date_invalid_years(self, data_source):
        """Test _is_valid_date with invalid years."""
        invalid_years = [2008, 2007, 1999]
        for year in invalid_years:
            assert data_source._is_valid_date(year, 1) is False
    
    @patch('src.data_sources.tlc_data_source.datetime')
    def test_is_valid_date_future_dates(self, mock_datetime, data_source):
        """Test _is_valid_date rejects future dates considering TLC delay."""
        # Mock current date as March 2024
        mock_now = datetime(2024, 3, 15)
        mock_datetime.now.return_value = mock_now
        
        # TLC publishes with ~2 month delay, so January 2024 should be max available
        assert data_source._is_valid_date(2024, 1) is True   # Should be available
        assert data_source._is_valid_date(2024, 2) is False  # Too recent
        assert data_source._is_valid_date(2024, 3) is False  # Current month
        assert data_source._is_valid_date(2024, 4) is False  # Future
    
    @patch('src.data_sources.tlc_data_source.datetime')
    def test_is_valid_date_year_boundary(self, mock_datetime, data_source):
        """Test _is_valid_date handles year boundaries correctly."""
        # Mock current date as February 2024
        mock_now = datetime(2024, 2, 15)
        mock_datetime.now.return_value = mock_now
        
        # With 2-month delay, December 2023 should be available
        assert data_source._is_valid_date(2023, 12) is True
        assert data_source._is_valid_date(2024, 1) is False  # Too recent