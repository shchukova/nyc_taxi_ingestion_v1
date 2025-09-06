# tests/unit/test_file_extractor_integration.py
"""Integration tests for FileExtractor combining multiple features."""

import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch
import requests

from src.extractors.file_extractor import FileExtractor
from src.config.settings import TLCConfig
from src.data_sources.tlc_data_source import TLCDataFile
from src.utils.exceptions import ExtractionError


class TestFileExtractorIntegration:
    """Integration tests combining multiple features."""
    
    @pytest.fixture
    def extractor(self):
        """Create FileExtractor for integration testing."""
        config = TLCConfig(
            max_retries=2,
            timeout_seconds=30
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            yield FileExtractor(config, Path(temp_dir))
    
    @pytest.fixture
    def large_data_file(self):
        """Create a large data file for testing."""
        return TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
            filename="yellow_tripdata_2024-01.parquet",
            estimated_size_mb=150
        )
    
    def test_full_download_workflow(self, extractor, large_data_file):
        """Test complete download workflow."""
        # Mock successful download
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': str(150 * 1024 * 1024)}  # 150MB
        
        # Create realistic file content
        chunk_size = 8192
        total_chunks = (150 * 1024 * 1024) // chunk_size
        mock_response.iter_content.return_value = [b'x' * chunk_size] * total_chunks
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            # Download file
            result_path = extractor.download_file(large_data_file, show_progress=True)
            
            # Verify file exists and has correct size
            assert result_path.exists()
            assert result_path.stat().st_size == 150 * 1024 * 1024
            
            # Get metadata
            metadata = extractor.get_file_metadata(result_path)
            assert metadata['size_mb'] == 150.0
            assert metadata['filename'] == large_data_file.filename
            assert len(metadata['md5_hash']) == 32
    
    def test_download_with_retry_success(self, extractor, large_data_file):
        """Test download succeeds after retry."""
        # First call fails, second succeeds
        mock_response_fail = Mock()
        mock_response_fail.status_code = 500
        mock_response_fail.raise_for_status.side_effect = requests.HTTPError("Server error")
        
        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.headers = {'content-length': '1024'}
        mock_response_success.iter_content.return_value = [b'test data']
        
        with patch.object(extractor._session, 'get', side_effect=[mock_response_fail, mock_response_success]):
            result_path = extractor.download_file(large_data_file)
            
            # Should succeed after retry
            assert result_path.exists()
            assert result_path.read_text() == "test data"
    
    def test_download_validation_and_metadata_workflow(self, extractor):
        """Test download, validation, and metadata extraction workflow."""
        data_file = TLCDataFile(
            trip_type="green_tripdata",
            year=2024,
            month=2,
            url="https://test.example.com/green_tripdata_2024-02.parquet",
            filename="green_tripdata_2024-02.parquet",
            estimated_size_mb=30
        )
        
        # Mock successful download
        test_content = b"parquet file content" * 1000  # ~20KB
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': str(len(test_content))}
        mock_response.iter_content.return_value = [test_content]
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            # Download file
            result_path = extractor.download_file(data_file)
            
            # File should exist and pass validation
            assert result_path.exists()
            assert extractor._validate_file_integrity(result_path, data_file)
            
            # Get and verify metadata
            metadata = extractor.get_file_metadata(result_path)
            assert metadata['size_bytes'] == len(test_content)
            assert metadata['filename'] == data_file.filename
            
            # Verify MD5
            import hashlib
            expected_md5 = hashlib.md5(test_content).hexdigest()
            assert metadata['md5_hash'] == expected_md5
    
    def test_url_verification_before_download(self, extractor, large_data_file):
        """Test URL verification before attempting download."""
        # Test accessible URL
        mock_head_response = Mock()
        mock_head_response.status_code = 200
        
        with patch.object(extractor._session, 'head', return_value=mock_head_response):
            accessible = extractor.verify_url_accessibility(large_data_file.url)
            assert accessible is True
        
        # Test inaccessible URL
        mock_head_response.status_code = 404
        with patch.object(extractor._session, 'head', return_value=mock_head_response):
            accessible = extractor.verify_url_accessibility(large_data_file.url)
            assert accessible is False
    
    def test_concurrent_download_simulation(self, extractor):
        """Test that extractor can handle multiple files (simulated concurrency)."""
        data_files = [
            TLCDataFile(
                trip_type="yellow_tripdata",
                year=2024,
                month=i,
                url=f"https://test.example.com/yellow_tripdata_2024-{i:02d}.parquet",
                filename=f"yellow_tripdata_2024-{i:02d}.parquet",
                estimated_size_mb=100
            )
            for i in range(1, 4)  # 3 files
        ]
        
        # Mock responses for each file
        def mock_get(url, **kwargs):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {'content-length': '1024'}
            # Different content for each file
            file_id = url.split('-')[-1].split('.')[0]  # Extract month
            mock_response.iter_content.return_value = [f"file_{file_id}_content".encode()]
            return mock_response
        
        with patch.object(extractor._session, 'get', side_effect=mock_get):
            downloaded_files = []
            
            # Download each file
            for data_file in data_files:
                result_path = extractor.download_file(data_file, show_progress=False)
                downloaded_files.append(result_path)
            
            # Verify all files were downloaded
            assert len(downloaded_files) == 3
            for path in downloaded_files:
                assert path.exists()
                assert path.stat().st_size > 0
    
    def test_error_recovery_and_cleanup(self, extractor, large_data_file):
        """Test error recovery and cleanup mechanisms."""
        # Create a scenario where download fails after partial completion
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '10000'}
        
        # Simulate network failure during download
        def failing_iter_content(chunk_size=8192):
            yield b'partial_data'
            raise requests.ConnectionError("Network failure")
        
        mock_response.iter_content.side_effect = failing_iter_content
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            # Download should fail and clean up
            with pytest.raises(ExtractionError):
                extractor.download_file(large_data_file)
            
            # Verify no partial files remain
            expected_path = extractor.data_dir / large_data_file.filename
            temp_path = expected_path.with_suffix('.tmp')
            
            assert not expected_path.exists()
            assert not temp_path.exists()
    
    def test_performance_tracking(self, extractor):
        """Test performance tracking and logging."""
        data_file = TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://test.example.com/test.parquet",
            filename="test.parquet",
            estimated_size_mb=50
        )
        
        # Mock response with controlled timing
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': str(50 * 1024 * 1024)}
        
        # Create chunks to simulate realistic download
        chunk_size = 1024 * 1024  # 1MB chunks
        total_chunks = 50
        mock_response.iter_content.return_value = [b'x' * chunk_size] * total_chunks
        
        with patch.object(extractor._session, 'get', return_value=mock_response):
            with patch.object(extractor.logger, 'info') as mock_log:
                # Download with progress tracking
                result_path = extractor.download_file(data_file, show_progress=True)
                
                # Verify performance logging occurred
                assert result_path.exists()
                
                # Check that completion message was logged
                log_calls = [call[0][0] for call in mock_log.call_args_list]
                completion_logs = [msg for msg in log_calls if "Download completed" in msg]
                assert len(completion_logs) >= 1
                
                # Check that speed was calculated
                completion_log = completion_logs[0]
                assert "MB/s" in completion_log