# tests/unit/test_pipeline_config.py
"""Tests for PipelineConfig dataclass."""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch
from src.config.settings import PipelineConfig


class TestPipelineConfig:
    """Test PipelineConfig dataclass functionality."""
    
    def test_pipeline_config_creation_with_string_path(self):
        """Test creating PipelineConfig with string data_dir."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = PipelineConfig(
                data_dir=temp_dir,
                batch_size=5000,
                max_workers=2,
                enable_data_validation=False,
                cleanup_temp_files=False,
                log_level="DEBUG"
            )
            
            assert isinstance(config.data_dir, Path)
            assert str(config.data_dir) == temp_dir
            assert config.batch_size == 5000
            assert config.max_workers == 2
            assert config.enable_data_validation is False
            assert config.cleanup_temp_files is False
            assert config.log_level == "DEBUG"
    
    def test_pipeline_config_creation_with_path_object(self):
        """Test creating PipelineConfig with Path object."""
        with tempfile.TemporaryDirectory() as temp_dir:
            path_obj = Path(temp_dir)
            config = PipelineConfig(data_dir=path_obj)
            
            assert isinstance(config.data_dir, Path)
            assert config.data_dir == path_obj
    
    def test_pipeline_config_with_defaults(self):
        """Test PipelineConfig default values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = PipelineConfig(data_dir=temp_dir)
            
            assert config.batch_size == 10000  # Default
            assert config.max_workers == 4  # Default
            assert config.enable_data_validation is True  # Default
            assert config.cleanup_temp_files is True  # Default
            assert config.log_level == "INFO"  # Default
    
    def test_pipeline_config_creates_directory(self):
        """Test that PipelineConfig creates data directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a subdirectory path that doesn't exist yet
            new_dir = Path(temp_dir) / "test_data"
            assert not new_dir.exists()
            
            config = PipelineConfig(data_dir=new_dir)
            
            # Directory should be created by __post_init__
            assert config.data_dir.exists()
            assert config.data_dir.is_dir()
    
    def test_pipeline_config_handles_existing_directory(self):
        """Test PipelineConfig with existing directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Directory already exists
            config = PipelineConfig(data_dir=temp_dir)
            
            assert config.data_dir.exists()
            assert config.data_dir.is_dir()