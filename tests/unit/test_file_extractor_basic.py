# tests/unit/test_file_extractor_basic.py
"""Basic tests for FileExtractor class."""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import requests

from src.extractors.file_extractor import FileExtractor
from src.config.settings import TLCConfig
from src.data_sources.tlc_data_source import TLCDataFile
from src.utils.exceptions import ExtractionError


class TestFileExtractorBasic:
    """Test basic FileExtractor functionality."""
    
    @pytest.fixture
    def tlc_config(self):
        """Create TLC config for testing."""
        return TLCConfig(
            base_url="https://test.example.com",
            max_retries=3,
            timeout_seconds=30
        )
    
    @pytest.fixture
    def temp_data_dir(self):
        """Create temporary directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def extractor(self, tlc_config, temp_data_dir):
        """Create FileExtractor instance for testing."""
        return FileExtractor(tlc_config, temp_data_dir)
    
    @pytest.fixture
    def sample_data_file(self):
        """Create sample TLCDataFile for testing."""
        return TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=3,
            url="https://test.example.com/yellow_tripdata_2024-03.parquet",
            filename="yellow_tripdata_2024-03.parquet",
            estimated_size_mb=150
        )
    
    def test_extractor_initialization(self, tlc_config, temp_data_dir):
        """Test FileExtractor initialization."""
        extractor = FileExtractor(tlc_config, temp_data_dir)
        
        assert extractor.config == tlc_config
        assert extractor.data_dir == temp_data_dir
        assert extractor.data_dir.exists()
        assert hasattr(extractor, 'logger')
        assert hasattr(extractor, '_session')
        assert isinstance(extractor._session, requests.Session)
    
    def test_data_directory_creation(self, tlc_config):
        """Test that data directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a subdirectory path that doesn't exist yet
            data_dir = Path(temp_dir) / "test_data" / "subdir"
            assert not data_dir.exists()
            
            extractor = FileExtractor(tlc_config, data_dir)
            
            # Directory should be created
            assert extractor.data_dir.exists()
            assert extractor.data_dir.is_dir()
    
    def test_session_configuration(self, extractor):
        """Test that HTTP session is configured correctly."""
        session = extractor._session
        
        # Check headers
        assert 'User-Agent' in session.headers
        assert 'NYC-Taxi-Data-Pipeline' in session.headers['User-Agent']
        assert session.headers['Accept'] == '*/*'
        assert session.headers['Connection'] == 'keep-alive'
        
        # Check that adapters are mounted
        assert 'http://' in session.adapters
        assert 'https://' in session.adapters
    
    def test_create_session_retry_strategy(self, extractor):
        """Test that retry strategy is configured correctly."""
        session = extractor._session
        
        # Get the HTTPAdapter to check retry configuration
        adapter = session.get_adapter('https://test.com')
        assert hasattr(adapter, 'max_retries')
        
        # Check that max_retries matches config
        retry_config = adapter.max_retries
        assert retry_config.total == extractor.config.max_retries