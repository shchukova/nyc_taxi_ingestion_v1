# tests/unit/test_settings_main.py
"""Tests for the main Settings class."""

import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch
from src.config.settings import Settings


class TestSettings:
    """Test the main Settings class that aggregates all configs."""
    
    def test_settings_initialization(self):
        """Test Settings class initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(os.environ, {'DATA_DIR': temp_dir}):
                settings = Settings()
                
                # Check that all config objects are created
                assert hasattr(settings, 'snowflake')
                assert hasattr(settings, 's3')
                assert hasattr(settings, 'tlc')
                assert hasattr(settings, 'pipeline')
                
                # Check types
                from src.config.settings import SnowflakeConfig, S3Config, TLCConfig, PipelineConfig
                assert isinstance(settings.snowflake, SnowflakeConfig)
                assert isinstance(settings.s3, S3Config)
                assert isinstance(settings.tlc, TLCConfig)
                assert isinstance(settings.pipeline, PipelineConfig)
    
    def test_settings_pipeline_config_from_environment(self):
        """Test that Settings loads pipeline config from environment."""
        env_vars = {
            'DATA_DIR': '/tmp/test_data',
            'BATCH_SIZE': '20000',
            'MAX_WORKERS': '8',
            'ENABLE_VALIDATION': 'false',
            'CLEANUP_TEMP_FILES': 'false',
            'LOG_LEVEL': 'DEBUG'
        }
        
        with patch.dict(os.environ, env_vars):
            settings = Settings()
            
            assert str(settings.pipeline.data_dir) == '/tmp/test_data'
            assert settings.pipeline.batch_size == 20000
            assert settings.pipeline.max_workers == 8
            assert settings.pipeline.enable_data_validation is False
            assert settings.pipeline.cleanup_temp_files is False
            assert settings.pipeline.log_level == 'DEBUG'
    
    def test_settings_pipeline_config_defaults(self):
        """Test Settings uses defaults for pipeline config."""
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings()
            
            assert str(settings.pipeline.data_dir) == 'data'  # Default
            assert settings.pipeline.batch_size == 10000  # Default
            assert settings.pipeline.max_workers == 4  # Default
            assert settings.pipeline.enable_data_validation is True  # Default
            assert settings.pipeline.cleanup_temp_files is True  # Default
            assert settings.pipeline.log_level == 'INFO'  # Default
    
    def test_settings_validate_success(self):
        """Test Settings validation passes with valid config."""
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USERNAME': 'test_user',
            'SNOWFLAKE_PASSWORD': 'test_password',
            'S3_BUCKET_NAME': 'test-bucket',
            'AWS_ACCESS_KEY_ID': 'test_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret'
        }
        
        with patch.dict(os.environ, env_vars):
            settings = Settings()
            assert settings.validate() is True
    
    def test_settings_validate_fails_missing_snowflake(self):
        """Test Settings validation fails with missing Snowflake config."""
        env_vars = {
            # Missing Snowflake credentials
            'S3_BUCKET_NAME': 'test-bucket',
            'AWS_ACCESS_KEY_ID': 'test_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            settings = Settings()
            assert settings.validate() is False
    
    def test_settings_validate_fails_missing_s3(self):
        """Test Settings validation fails with missing S3 config."""
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USERNAME': 'test_user',
            'SNOWFLAKE_PASSWORD': 'test_password'
            # Missing S3 credentials
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            settings = Settings()
            assert settings.validate() is False
    
    def test_settings_validate_fails_partial_snowflake_config(self):
        """Test validation fails with partial Snowflake config."""
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USERNAME': 'test_user',
            # Missing password
            'S3_BUCKET_NAME': 'test-bucket',
            'AWS_ACCESS_KEY_ID': 'test_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret'
        }
        
        with patch.dict(os.environ, env_vars):
            settings = Settings()
            assert settings.validate() is False
    
    def test_settings_validate_fails_partial_s3_config(self):
        """Test validation fails with partial S3 config."""
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USERNAME': 'test_user',
            'SNOWFLAKE_PASSWORD': 'test_password',
            'S3_BUCKET_NAME': 'test-bucket',
            'AWS_ACCESS_KEY_ID': 'test_key'
            # Missing secret key
        }
        
        with patch.dict(os.environ, env_vars):
            settings = Settings()
            assert settings.validate() is False