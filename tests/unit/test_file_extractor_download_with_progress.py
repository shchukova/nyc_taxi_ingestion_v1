# tests/unit/test_file_extractor_download_with_progress.py
"""Tests for download with progress functionality."""

import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import requests

from src.extractors.file_extractor import FileExtractor
from src.config.settings import TLCConfig
from src.utils.exceptions import ExtractionError


class TestFileExtractorDownloadWithProgress:
    """Test download with progress tracking."""
    
    @pytest.fixture
    def extractor(self):
        """Create FileExtractor for testing."""
        config = TLCConfig(timeout_seconds=30)
        with tempfile.TemporaryDirectory() as temp_dir:
            yield FileExtractor(config, Path(temp_dir))
    
    def test_download_with_progress_success(self, extractor):
        """Test successful download with progress tracking."""
        test_url = "https://test.example.com/test.parquet"
        local_path = extractor.data_dir / "test.parquet"
        
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '1024'}
        mock_response.iter_content.return_value = [b'a' * 512, b'b' * 512]
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            extractor._download_with_progress(test_url, local_path, show_progress=True)
            
            # Verify file was created
            assert local_path.exists()
            assert local_path.read_bytes() == b'a' * 512 + b'b' * 512
    
    def test_download_with_progress_no_content_length(self, extractor):
        """Test download when content-length header is missing."""
        test_url = "https://test.example.com/test.parquet"
        local_path = extractor.data_dir / "test.parquet"
        
        # Mock response without content-length
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}  # No content-length
        mock_response.iter_content.return_value = [b'test data']
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            extractor._download_with_progress(test_url, local_path, show_progress=True)
            
            assert local_path.exists()
            assert local_path.read_text() == "test data"
    
    def test_download_with_progress_network_error(self, extractor):
        """Test download with network error."""
        test_url = "https://test.example.com/test.parquet"
        local_path = extractor.data_dir / "test.parquet"
        
        with patch.object(extractor._session, 'get', side_effect=requests.ConnectionError("Network error")):
            with pytest.raises(ExtractionError, match="Network error"):
                extractor._download_with_progress(test_url, local_path)
    
    def test_download_with_progress_http_error(self, extractor):
        """Test download with HTTP error status."""
        test_url = "https://test.example.com/test.parquet"
        local_path = extractor.data_dir / "test.parquet"
        
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            with pytest.raises(ExtractionError, match="Network error"):
                extractor._download_with_progress(test_url, local_path)
    
    def test_download_with_progress_file_io_error(self, extractor):
        """Test download with file I/O error."""
        test_url = "https://test.example.com/test.parquet"
        local_path = extractor.data_dir / "test.parquet"
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '1024'}
        mock_response.iter_content.return_value = [b'test data']
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            with patch('builtins.open', side_effect=IOError("Permission denied")):
                with pytest.raises(ExtractionError, match="File I/O error"):
                    extractor._download_with_progress(test_url, local_path)
    
    def test_download_creates_temp_file_first(self, extractor):
        """Test that download creates temporary file first."""
        test_url = "https://test.example.com/test.parquet"
        local_path = extractor.data_dir / "test.parquet"
        temp_path = local_path.with_suffix(local_path.suffix + '.tmp')
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '1024'}
        mock_response.iter_content.return_value = [b'test data']
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            extractor._download_with_progress(test_url, local_path)
            
            # Temp file should be renamed to final file
            assert local_path.exists()
            assert not temp_path.exists()
    
    @patch('time.time')
    def test_log_progress_called_periodically(self, mock_time, extractor):
        """Test that progress is logged periodically."""
        test_url = "https://test.example.com/test.parquet"
        local_path = extractor.data_dir / "test.parquet"
    
        # Mock time to simulate progress intervals - PROVIDE MORE VALUES
        # The code calls time.time() multiple times:
        # 1. start_time = time.time()
        # 2. last_progress_time = start_time  
        # 3. time.time() - last_progress_time > 5 (for each chunk)
        # 4. elapsed_time = time.time() - start_time (at the end)
        mock_time.side_effect = [
            0,    # start_time
            0,    # last_progress_time = start_time
            1,    # First chunk check
            6,    # Second chunk check (triggers progress log)
            6,    # Update last_progress_time
            7,    # Third chunk check  
            12,   # Fourth chunk check (triggers progress log)
            12,   # Update last_progress_time
            13,   # Final elapsed_time calculation
            13    # Extra buffer values
        ]
    
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '1024'}
        mock_response.iter_content.return_value = [b'a' * 256] * 4  # 4 chunks
    
        with patch.object(extractor._session, 'get', return_value=mock_response):
            with patch.object(extractor, '_log_progress') as mock_log_progress:
                extractor._download_with_progress(test_url, local_path, show_progress=True)
            
                # Progress should be logged at least once
                assert mock_log_progress.call_count >= 1    