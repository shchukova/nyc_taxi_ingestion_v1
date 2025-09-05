# tests/unit/test_global_settings.py
"""Tests for the global settings instance."""

import pytest
from src.config.settings import settings


class TestGlobalSettings:
    """Test the global settings instance."""
    
    def test_global_settings_exists(self):
        """Test that global settings instance is available."""
        assert settings is not None
        assert hasattr(settings, 'snowflake')
        assert hasattr(settings, 's3')
        assert hasattr(settings, 'tlc')
        assert hasattr(settings, 'pipeline')
    
    def test_global_settings_is_settings_instance(self):
        """Test that global settings is instance of Settings class."""
        from src.config.settings import Settings
        assert isinstance(settings, Settings)
    
    def test_global_settings_has_validate_method(self):
        """Test that global settings has validate method."""
        assert hasattr(settings, 'validate')
        assert callable(settings.validate)
        
        # Should return boolean
        result = settings.validate()
        assert isinstance(result, bool)