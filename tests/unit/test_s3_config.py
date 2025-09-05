"""Tests for S3Config dataclass."""

import pytest
import os
from unittest.mock import patch
from src.config.settings import S3Config


class TestS3Config:
    """Test S3Config dataclass functionality."""
    
    def test_s3_config_creation_with_all_fields(self):
        """Test creating S3Config with all fields."""
        config = S3Config(
            bucket_name='test-bucket',
            region='us-west-2',
            access_key_id='test_key',
            secret_access_key='test_secret',
            prefix='custom-prefix'
        )
        
        assert config.bucket_name == 'test-bucket'
        assert config.region == 'us-west-2'
        assert config.access_key_id == 'test_key'
        assert config.secret_access_key == 'test_secret'
        assert config.prefix == 'custom-prefix'
    
    def test_s3_config_creation_with_default_prefix(self):
        """Test S3Config uses default prefix when not specified."""
        config = S3Config(
            bucket_name='test-bucket',
            region='us-east-1',
            access_key_id='test_key',
            secret_access_key='test_secret'
        )
        
        assert config.prefix == 'taxi-data'  # Default value
    
    def test_s3_config_from_env_with_all_variables(self):
        """Test loading S3Config from environment variables."""
        env_vars = {
            'S3_BUCKET_NAME': 'env-bucket',
            'S3_REGION': 'eu-west-1',
            'AWS_ACCESS_KEY_ID': 'env_key',
            'AWS_SECRET_ACCESS_KEY': 'env_secret',
            'S3_PREFIX': 'env-prefix'
        }
        
        with patch.dict(os.environ, env_vars):
            config = S3Config.from_env()
            
            assert config.bucket_name == 'env-bucket'
            assert config.region == 'eu-west-1'
            assert config.access_key_id == 'env_key'
            assert config.secret_access_key == 'env_secret'
            assert config.prefix == 'env-prefix'
    
    def test_s3_config_from_env_with_defaults(self):
        """Test S3Config uses defaults when env vars not set."""
        env_vars = {
            'AWS_ACCESS_KEY_ID': 'test_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret'
            # Missing bucket, region, prefix
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            config = S3Config.from_env()
            
            assert config.access_key_id == 'test_key'
            assert config.secret_access_key == 'test_secret'
            assert config.bucket_name == ''  # Default empty
            assert config.region == 'us-east-1'  # Default
            assert config.prefix == 'taxi-data'  # Default
    
    def test_s3_config_from_env_empty_environment(self):
        """Test S3Config with completely empty environment."""
        with patch.dict(os.environ, {}, clear=True):
            config = S3Config.from_env()
            
            assert config.bucket_name == ''
            assert config.region == 'us-east-1'
            assert config.access_key_id == ''
            assert config.secret_access_key == ''
            assert config.prefix == 'taxi-data'
