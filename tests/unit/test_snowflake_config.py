# tests/unit/test_snowflake_config.py
"""Tests for SnowflakeConfig dataclass."""

import pytest
import os
from unittest.mock import patch
from src.config.settings import SnowflakeConfig


class TestSnowflakeConfig:
    """Test SnowflakeConfig dataclass functionality."""
    
    def test_snowflake_config_creation_with_all_fields(self):
        """Test creating SnowflakeConfig with all fields."""
        config = SnowflakeConfig(
            account='test_account',
            username='test_user',
            password='test_password',
            warehouse='TEST_WH',
            database='TEST_DB',
            schema='TEST_SCHEMA',
            role='TEST_ROLE'
        )
        
        assert config.account == 'test_account'
        assert config.username == 'test_user'
        assert config.password == 'test_password'
        assert config.warehouse == 'TEST_WH'
        assert config.database == 'TEST_DB'
        assert config.schema == 'TEST_SCHEMA'
        assert config.role == 'TEST_ROLE'
    
    def test_snowflake_config_creation_without_optional_fields(self):
        """Test creating SnowflakeConfig without optional role field."""
        config = SnowflakeConfig(
            account='test_account',
            username='test_user',
            password='test_password',
            warehouse='TEST_WH',
            database='TEST_DB',
            schema='TEST_SCHEMA'
        )
        
        assert config.role is None
    
    def test_snowflake_config_from_env_with_all_variables(self):
        """Test loading SnowflakeConfig from environment variables."""
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'env_account',
            'SNOWFLAKE_USERNAME': 'env_user',
            'SNOWFLAKE_PASSWORD': 'env_password',
            'SNOWFLAKE_WAREHOUSE': 'ENV_WH',
            'SNOWFLAKE_DATABASE': 'ENV_DB',
            'SNOWFLAKE_SCHEMA': 'ENV_SCHEMA',
            'SNOWFLAKE_ROLE': 'ENV_ROLE'
        }
        
        with patch.dict(os.environ, env_vars):
            config = SnowflakeConfig.from_env()
            
            assert config.account == 'env_account'
            assert config.username == 'env_user'
            assert config.password == 'env_password'
            assert config.warehouse == 'ENV_WH'
            assert config.database == 'ENV_DB'
            assert config.schema == 'ENV_SCHEMA'
            assert config.role == 'ENV_ROLE'
    
    def test_snowflake_config_from_env_with_defaults(self):
        """Test SnowflakeConfig uses defaults when env vars not set."""
        env_vars = {
            'SNOWFLAKE_ACCOUNT': 'test_account',
            'SNOWFLAKE_USERNAME': 'test_user',
            'SNOWFLAKE_PASSWORD': 'test_password'
            # Missing warehouse, database, schema, role
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowflakeConfig.from_env()
            
            assert config.account == 'test_account'
            assert config.username == 'test_user'
            assert config.password == 'test_password'
            assert config.warehouse == 'COMPUTE_WH'  # Default
            assert config.database == 'NYC_TAXI_DB'  # Default
            assert config.schema == 'RAW'  # Default
            assert config.role is None  # Default for optional field
    
    def test_snowflake_config_from_env_empty_environment(self):
        """Test SnowflakeConfig with completely empty environment."""
        with patch.dict(os.environ, {}, clear=True):
            config = SnowflakeConfig.from_env()
            
            assert config.account == ''
            assert config.username == ''
            assert config.password == ''
            assert config.warehouse == 'COMPUTE_WH'
            assert config.database == 'NYC_TAXI_DB'
            assert config.schema == 'RAW'
            assert config.role is None
