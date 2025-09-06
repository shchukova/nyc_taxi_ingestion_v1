# tests/unit/test_file_extractor_context_manager.py
"""Tests for FileExtractor context manager functionality."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.extractors.file_extractor import FileExtractor
from src.config.settings import TLCConfig


class TestFileExtractorContextManager:
    """Test context manager functionality."""
    
    def test_context_manager_entry_and_exit(self):
        """Test context manager entry and exit."""
        config = TLCConfig()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            with FileExtractor(config, Path(temp_dir)) as extractor:
                # Test that extractor is properly initialized
                assert isinstance(extractor, FileExtractor)
                assert extractor.config == config
                assert extractor._session is not None
    
    def test_context_manager_cleanup_on_exit(self):
        """Test that resources are cleaned up on context exit."""
        config = TLCConfig()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)
            
            # Create some temp files to test cleanup
            temp_file = data_dir / "test.tmp"
            temp_file.write_text("temporary data")
            
            with patch('src.extractors.file_extractor.FileExtractor.cleanup_temp_files') as mock_cleanup:
                with FileExtractor(config, data_dir) as extractor:
                    # Mock session close to verify it's called
                    with patch.object(extractor._session, 'close') as mock_close:
                        pass
                
                # Verify cleanup methods were called on exit
                mock_cleanup.assert_called_once()
                mock_close.assert_called_once()
    
    def test_context_manager_exception_handling(self):
        """Test context manager handles exceptions properly."""
        config = TLCConfig()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch('src.extractors.file_extractor.FileExtractor.cleanup_temp_files') as mock_cleanup:
                try:
                    with FileExtractor(config, Path(temp_dir)) as extractor:
                        # Simulate an exception inside the context
                        raise ValueError("Test exception")
                except ValueError:
                    pass
                
                # Cleanup should still be called even when exception occurs
                mock_cleanup.assert_called_once()