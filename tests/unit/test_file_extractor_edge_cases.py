# tests/unit/test_file_extractor_edge_cases.py
"""Tests for edge cases and error conditions in FileExtractor."""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch
import requests

from src.extractors.file_extractor import FileExtractor
from src.config.settings import TLCConfig
from src.data_sources.tlc_data_source import TLCDataFile
from src.utils.exceptions import ExtractionError


class TestFileExtractorEdgeCases:
    """Test edge cases and error conditions."""
    
    @pytest.fixture
    def extractor(self):
        """Create FileExtractor for edge case testing."""
        config = TLCConfig(max_retries=1, timeout_seconds=5)
        with tempfile.TemporaryDirectory() as temp_dir:
            yield FileExtractor(config, Path(temp_dir))
    
    def test_download_zero_byte_file(self, extractor):
        """Test downloading a zero-byte file."""
        data_file = TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://test.example.com/empty.parquet",
            filename="empty.parquet",
            estimated_size_mb=100
        )
        
        # Mock response with no content
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '0'}
        mock_response.iter_content.return_value = []
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            with pytest.raises(ExtractionError, match="failed integrity check"):
                extractor.download_file(data_file)
    
    def test_download_with_no_content_length_header(self, extractor):
        """Test download when server doesn't provide content-length."""
        data_file = TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://test.example.com/test.parquet",
            filename="test.parquet"
        )
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}  # No content-length
        mock_response.iter_content.return_value = [b'test content']
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            result_path = extractor.download_file(data_file)
            assert result_path.exists()
            assert result_path.read_text() == "test content"
    
    def test_download_with_very_large_file(self, extractor):
        """Test download progress tracking with very large file."""
        data_file = TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://test.example.com/huge.parquet",
            filename="huge.parquet",
            estimated_size_mb=1000  # 1GB
        )
        
        # Mock response with large file
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': str(1000 * 1024 * 1024)}
        
        # Simulate large file with many small chunks
        def generate_chunks():
            for _ in range(100):  # 100 chunks of 10MB each
                yield b'x' * (10 * 1024 * 1024)
        
        mock_response.iter_content.return_value = generate_chunks()
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            with patch.object(extractor, '_log_progress') as mock_log_progress:
                result_path = extractor.download_file(data_file, show_progress=True)
                
                # Should have logged progress multiple times
                assert mock_log_progress.call_count >= 1
                assert result_path.exists()
    
    def test_download_with_connection_timeout(self, extractor):
        """Test download with connection timeout."""
        data_file = TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://slow.example.com/test.parquet",
            filename="test.parquet"
        )
        
        with patch.object(extractor._session, 'get', side_effect=requests.Timeout("Request timeout")):
            with pytest.raises(ExtractionError, match="Network error"):
                extractor.download_file(data_file)
    
    def test_download_with_ssl_error(self, extractor):
        """Test download with SSL error."""
        data_file = TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://bad-ssl.example.com/test.parquet",
            filename="test.parquet"
        )
        
        with patch.object(extractor._session, 'get', side_effect=requests.exceptions.SSLError("SSL error")):
            with pytest.raises(ExtractionError, match="Network error"):
                extractor.download_file(data_file)
    
    def test_download_with_disk_full_error(self, extractor):
        """Test download when disk is full."""
        data_file = TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://test.example.com/test.parquet",
            filename="test.parquet"
        )
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '1024'}
        mock_response.iter_content.return_value = [b'test data']
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            with patch('builtins.open', side_effect=OSError("No space left on device")):
                with pytest.raises(ExtractionError, match="File I/O error"):
                    extractor.download_file(data_file)
    
    def test_download_with_permission_denied(self, extractor):
        """Test download when write permission is denied."""
        data_file = TLCDataFile(
        trip_type="yellow_tripdata",
        year=2024,
        month=1,
        url="https://test.example.com/test.parquet",
        filename="test.parquet"
        )

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '1024'}
        mock_response.iter_content.return_value = [b'test data']
    
        # OPTION 1: Mock the file operations to simulate permission error
        with patch.object(extractor._session, 'get', return_value=mock_response):
            with patch('builtins.open', side_effect=PermissionError("Permission denied")):
                with pytest.raises(ExtractionError, match="File I/O error"):
                    extractor.download_file(data_file)

    
    def test_validate_file_with_special_characters_in_path(self, extractor):
        """Test file validation with special characters in path."""
        data_file = TLCDataFile(
            trip_type="test_data",
            year=2024,
            month=1,
            url="https://test.example.com/test.parquet",
            filename="test file with spaces & symbols!.parquet"
        )
        
        # Create file with special characters in name
        special_file = extractor.data_dir / data_file.filename
        special_file.write_text("test content")
        
        result = extractor._validate_file_integrity(special_file, data_file)
        assert result is True
    
    def test_metadata_calculation_for_binary_file(self, extractor):
        """Test metadata calculation for binary file."""
        # Create binary file with various byte values
        binary_file = extractor.data_dir / "binary.parquet"
        binary_content = bytes(range(256)) * 100  # 25.6KB of binary data
        binary_file.write_bytes(binary_content)
        
        metadata = extractor.get_file_metadata(binary_file)
        
        assert metadata['size_bytes'] == len(binary_content)
        assert metadata['size_mb'] == len(binary_content) / (1024 * 1024)
        assert len(metadata['md5_hash']) == 32
        
        # Verify MD5 is correct for binary content
        import hashlib
        expected_md5 = hashlib.md5(binary_content).hexdigest()
        assert metadata['md5_hash'] == expected_md5
    
    def test_cleanup_with_nested_temp_files(self, extractor):
        """Test cleanup with temp files in subdirectories."""
        # Create subdirectory with temp files
        subdir = extractor.data_dir / "subdir"
        subdir.mkdir()
        
        temp_file1 = extractor.data_dir / "file1.tmp"
        temp_file2 = subdir / "file2.tmp"  # In subdirectory
        regular_file = extractor.data_dir / "regular.parquet"
        
        temp_file1.write_text("temp1")
        temp_file2.write_text("temp2")
        regular_file.write_text("regular")
        
        # Cleanup should only find top-level temp files
        cleaned_count = extractor.cleanup_temp_files()
        
        assert cleaned_count == 1  # Only top-level temp file
        assert not temp_file1.exists()
        assert temp_file2.exists()  # Subdirectory temp file remains
        assert regular_file.exists()