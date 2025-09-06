# tests/unit/test_file_extractor_download.py
"""Tests for FileExtractor download functionality."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open
import requests

from src.extractors.file_extractor import FileExtractor
from src.config.settings import TLCConfig
from src.data_sources.tlc_data_source import TLCDataFile
from src.utils.exceptions import ExtractionError


class TestFileExtractorDownload:
    """Test download functionality."""
    
    @pytest.fixture
    def extractor(self):
        """Create FileExtractor for testing."""
        config = TLCConfig(max_retries=3, timeout_seconds=30)
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
    
    @patch('src.extractors.file_extractor.FileExtractor._validate_file_integrity')
    @patch('src.extractors.file_extractor.FileExtractor._download_with_progress')
    def test_download_file_success(self, mock_download, mock_validate, extractor, sample_data_file):
        """Test successful file download."""
        # Setup mocks
        mock_download.return_value = None
        mock_validate.return_value = True
        
        # Call download
        result_path = extractor.download_file(sample_data_file)
        
        # Verify results
        expected_path = extractor.data_dir / sample_data_file.filename
        assert result_path == expected_path
        
        # Verify method calls
        mock_download.assert_called_once_with(
            sample_data_file.url, 
            expected_path, 
            True  # show_progress
        )
        mock_validate.assert_called_once_with(expected_path, sample_data_file)
    
    def test_download_file_already_exists_valid(self, extractor, sample_data_file):
        """Test download when file already exists and is valid."""
        # Create existing file
        existing_file = extractor.data_dir / sample_data_file.filename
        existing_file.write_text("test data")
        
        with patch.object(extractor, '_validate_file_integrity', return_value=True) as mock_validate:
            with patch.object(extractor, '_download_with_progress') as mock_download:
                result_path = extractor.download_file(sample_data_file)
                
                # Should return existing file without downloading
                assert result_path == existing_file
                mock_validate.assert_called_once()
                mock_download.assert_not_called()
    
    def test_download_file_force_redownload(self, extractor, sample_data_file):
        """Test force redownload of existing file."""
        # Create existing file
        existing_file = extractor.data_dir / sample_data_file.filename
        existing_file.write_text("old data")
        
        with patch.object(extractor, '_validate_file_integrity', return_value=True):
            with patch.object(extractor, '_download_with_progress') as mock_download:
                result_path = extractor.download_file(
                    sample_data_file, 
                    force_redownload=True
                )
                
                # Should download even if file exists
                assert result_path == existing_file
                mock_download.assert_called_once()
    
    def test_download_file_existing_corrupted(self, extractor, sample_data_file):
        """Test download when existing file is corrupted."""
        # Create existing file
        existing_file = extractor.data_dir / sample_data_file.filename
        existing_file.write_text("corrupted data")
        
        with patch.object(extractor, '_validate_file_integrity', side_effect=[False, True]):
            with patch.object(extractor, '_download_with_progress') as mock_download:
                result_path = extractor.download_file(sample_data_file)
                
                # Should re-download corrupted file
                assert result_path == existing_file
                mock_download.assert_called_once()
    
    @patch('src.extractors.file_extractor.FileExtractor._download_with_progress')
    @patch('src.extractors.file_extractor.FileExtractor._validate_file_integrity')
    def test_download_file_validation_fails(self, mock_validate, mock_download, extractor, sample_data_file):
        """Test download when validation fails."""
        mock_download.return_value = None
        mock_validate.return_value = False
        
        with pytest.raises(ExtractionError, match="failed integrity check"):
            extractor.download_file(sample_data_file)
    
    @patch('src.extractors.file_extractor.FileExtractor._download_with_progress')
    def test_download_file_download_exception(self, mock_download, extractor, sample_data_file):
        """Test download when download raises exception."""
        mock_download.side_effect = requests.RequestException("Network error")
        
        with pytest.raises(ExtractionError, match="Failed to download"):
            extractor.download_file(sample_data_file)
    
    def test_download_file_cleanup_on_failure(self, extractor, sample_data_file):
        """Test that partial downloads are cleaned up on failure."""
        with patch.object(extractor, '_download_with_progress') as mock_download:
            with patch.object(extractor, '_validate_file_integrity', return_value=False):
                # Create partial file
                partial_file = extractor.data_dir / sample_data_file.filename
                partial_file.write_text("partial data")
                
                try:
                    extractor.download_file(sample_data_file)
                except ExtractionError:
                    pass
                
                # File should be cleaned up
                assert not partial_file.exists()