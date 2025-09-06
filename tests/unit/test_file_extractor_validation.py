# tests/unit/test_file_extractor_validation.py
"""Tests for file validation functionality."""

import pytest
import tempfile
from pathlib import Path

from src.extractors.file_extractor import FileExtractor
from src.config.settings import TLCConfig
from src.data_sources.tlc_data_source import TLCDataFile


class TestFileExtractorValidation:
    """Test file validation functionality."""
    
    @pytest.fixture
    def extractor(self):
        """Create FileExtractor for testing."""
        config = TLCConfig()
        with tempfile.TemporaryDirectory() as temp_dir:
            yield FileExtractor(config, Path(temp_dir))
    
    @pytest.fixture
    def sample_data_file(self):
        """Create sample data file for testing."""
        return TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=3,
            url="https://test.example.com/test.parquet",
            filename="test.parquet",
            estimated_size_mb=100
        )
    
    def test_validate_file_integrity_nonexistent_file(self, extractor, sample_data_file):
        """Test validation of non-existent file."""
        non_existent_path = extractor.data_dir / "nonexistent.parquet"
        
        result = extractor._validate_file_integrity(non_existent_path, sample_data_file)
        assert result is False
    
    def test_validate_file_integrity_empty_file(self, extractor, sample_data_file):
        """Test validation of empty file."""
        empty_file = extractor.data_dir / "empty.parquet"
        empty_file.touch()  # Creates empty file
        
        result = extractor._validate_file_integrity(empty_file, sample_data_file)
        assert result is False
    
    def test_validate_file_integrity_valid_file(self, extractor, sample_data_file):
        """Test validation of valid file."""
        valid_file = extractor.data_dir / "valid.parquet"
        # Create file with reasonable size (1MB)
        valid_file.write_bytes(b'x' * (1024 * 1024))
        
        result = extractor._validate_file_integrity(valid_file, sample_data_file)
        assert result is True
    
    def test_validate_file_integrity_size_warning(self, extractor, sample_data_file):
        """Test validation warns about unexpected file size."""
        oversized_file = extractor.data_dir / "oversized.parquet"
        # Create file much larger than expected (200MB vs 100MB expected)
        oversized_file.write_bytes(b'x' * (200 * 1024 * 1024))
        
        # Should still pass validation (just warning)
        result = extractor._validate_file_integrity(oversized_file, sample_data_file)
        assert result is True
    
    def test_validate_file_integrity_no_estimated_size(self, extractor):
        """Test validation when no estimated size is provided."""
        data_file = TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=3,
            url="https://test.example.com/test.parquet",
            filename="test.parquet"
            # No estimated_size_mb
        )
        
        valid_file = extractor.data_dir / "test.parquet"
        valid_file.write_bytes(b'test data')
        
        result = extractor._validate_file_integrity(valid_file, data_file)
        assert result is True