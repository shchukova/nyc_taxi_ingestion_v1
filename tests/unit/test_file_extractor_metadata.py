# tests/unit/test_file_extractor_metadata.py
"""Tests for file metadata functionality."""

import pytest
import tempfile
import hashlib
from pathlib import Path

from src.extractors.file_extractor import FileExtractor
from src.config.settings import TLCConfig
from src.utils.exceptions import ExtractionError


class TestFileExtractorMetadata:
    """Test file metadata functionality."""
    
    @pytest.fixture
    def extractor(self):
        """Create FileExtractor for testing."""
        config = TLCConfig()
        with tempfile.TemporaryDirectory() as temp_dir:
            yield FileExtractor(config, Path(temp_dir))
    
    def test_get_file_metadata_success(self, extractor):
        """Test getting metadata for existing file."""
        test_file = extractor.data_dir / "test.parquet"
        test_content = b"test file content"
        test_file.write_bytes(test_content)
        
        metadata = extractor.get_file_metadata(test_file)
        
        # Check all expected keys
        expected_keys = [
            'filename', 'file_path', 'size_bytes', 'size_mb',
            'created_time', 'modified_time', 'md5_hash'
        ]
        for key in expected_keys:
            assert key in metadata
        
        # Check specific values
        assert metadata['filename'] == 'test.parquet'
        assert metadata['size_bytes'] == len(test_content)
        assert metadata['size_mb'] == len(test_content) / (1024 * 1024)
        assert str(test_file) in metadata['file_path']
        
        # Check MD5 hash
        expected_md5 = hashlib.md5(test_content).hexdigest()
        assert metadata['md5_hash'] == expected_md5
    
    def test_get_file_metadata_nonexistent_file(self, extractor):
        """Test getting metadata for non-existent file."""
        non_existent_file = extractor.data_dir / "nonexistent.parquet"
        
        with pytest.raises(ExtractionError, match="File does not exist"):
            extractor.get_file_metadata(non_existent_file)
    
    def test_calculate_md5_success(self, extractor):
        """Test MD5 calculation."""
        test_file = extractor.data_dir / "test.txt"
        test_content = b"Hello, World!"
        test_file.write_bytes(test_content)
        
        md5_hash = extractor._calculate_md5(test_file)
        
        # Verify against known MD5
        expected_md5 = hashlib.md5(test_content).hexdigest()
        assert md5_hash == expected_md5
        assert len(md5_hash) == 32  # MD5 is 32 hex characters
    
    def test_calculate_md5_large_file(self, extractor):
        """Test MD5 calculation for larger file (tests chunked reading)."""
        test_file = extractor.data_dir / "large_test.txt"
        # Create file larger than chunk size (4096 bytes)
        test_content = b"A" * 10000
        test_file.write_bytes(test_content)
        
        md5_hash = extractor._calculate_md5(test_file)
        
        # Verify against known MD5
        expected_md5 = hashlib.md5(test_content).hexdigest()
        assert md5_hash == expected_md5
    
    def test_calculate_md5_empty_file(self, extractor):
        """Test MD5 calculation for empty file."""
        test_file = extractor.data_dir / "empty.txt"
        test_file.touch()  # Create empty file
        
        md5_hash = extractor._calculate_md5(test_file)
        
        # MD5 of empty file
        expected_md5 = hashlib.md5(b"").hexdigest()
        assert md5_hash == expected_md5