# tests/unit/test_file_extractor_utilities.py
"""Tests for utility functions in FileExtractor."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock

from src.extractors.file_extractor import FileExtractor
from src.config.settings import TLCConfig


class TestFileExtractorUtilities:
    """Test utility functions."""
    
    @pytest.fixture
    def extractor(self):
        """Create FileExtractor for testing."""
        config = TLCConfig()
        with tempfile.TemporaryDirectory() as temp_dir:
            yield FileExtractor(config, Path(temp_dir))
    
    def test_cleanup_temp_files_success(self, extractor):
        """Test cleaning up temporary files."""
        # Create some temp files
        temp_file1 = extractor.data_dir / "file1.parquet.tmp"
        temp_file2 = extractor.data_dir / "file2.parquet.tmp"
        regular_file = extractor.data_dir / "regular.parquet"
        
        temp_file1.write_text("temp1")
        temp_file2.write_text("temp2")
        regular_file.write_text("regular")
        
        cleaned_count = extractor.cleanup_temp_files()
        
        # Should clean up 2 temp files
        assert cleaned_count == 2
        assert not temp_file1.exists()
        assert not temp_file2.exists()
        assert regular_file.exists()  # Regular file should remain
    
    def test_cleanup_temp_files_no_temp_files(self, extractor):
        """Test cleanup when no temp files exist."""
        # Create only regular files
        regular_file = extractor.data_dir / "regular.parquet"
        regular_file.write_text("regular")
        
        cleaned_count = extractor.cleanup_temp_files()
        
        assert cleaned_count == 0
        assert regular_file.exists()
    
    def test_cleanup_temp_files_with_error(self, extractor):
        """Test cleanup when file deletion fails."""
        temp_file = extractor.data_dir / "file.parquet.tmp"
        temp_file.write_text("temp")
        
        # Mock unlink to raise exception
        with patch.object(Path, 'unlink', side_effect=PermissionError("Access denied")):
            cleaned_count = extractor.cleanup_temp_files()
            
            # Should handle error gracefully
            assert cleaned_count == 0
    
    def test_verify_url_accessibility_success(self, extractor):
        """Test URL accessibility check - success."""
        test_url = "https://test.example.com/file.parquet"
        
        mock_response = Mock()
        mock_response.status_code = 200
        
        with patch.object(extractor._session, 'head', return_value=mock_response):
            result = extractor.verify_url_accessibility(test_url)
            assert result is True
    
    def test_verify_url_accessibility_not_found(self, extractor):
        """Test URL accessibility check - 404."""
        test_url = "https://test.example.com/nonexistent.parquet"
        
        mock_response = Mock()
        mock_response.status_code = 404
        
        with patch.object(extractor._session, 'head', return_value=mock_response):
            result = extractor.verify_url_accessibility(test_url)
            assert result is False
    
    def test_verify_url_accessibility_network_error(self, extractor):
        """Test URL accessibility check - network error."""
        test_url = "https://test.example.com/file.parquet"
        
        with patch.object(extractor._session, 'head', side_effect=ConnectionError("Network error")):
            result = extractor.verify_url_accessibility(test_url)
            assert result is False
    
    def test_log_progress_with_total_size(self, extractor):
        """Test progress logging with known total size."""
        with patch.object(extractor.logger, 'info') as mock_log:
            extractor._log_progress(
                downloaded=50 * 1024 * 1024,  # 50MB
                total_size=100 * 1024 * 1024,  # 100MB
                start_time=0  # Assume time.time() returns 10
            )
            
            mock_log.assert_called_once()
            log_message = mock_log.call_args[0][0]
            
            # Check that progress percentage is in the message
            assert "50.0%" in log_message
            assert "Speed:" in log_message
    
    def test_log_progress_without_total_size(self, extractor):
        """Test progress logging without known total size."""
        with patch.object(extractor.logger, 'info') as mock_log:
            extractor._log_progress(
                downloaded=50 * 1024 * 1024,  # 50MB
                total_size=0,  # Unknown size
                start_time=0
            )
            
            mock_log.assert_called_once()
            log_message = mock_log.call_args[0][0]
            
            # Should show downloaded amount without percentage
            assert "Downloaded:" in log_message
            assert "%" not in log_message
            assert "Speed:" in log_message