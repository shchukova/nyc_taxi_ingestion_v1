# tests/unit/test_snowflake_loader_edge_cases.py
"""
Edge case and performance tests for SnowflakeLoader class
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile
import os
from datetime import datetime, timedelta

from src.loaders.snowflake_loader import SnowflakeLoader
from src.utils.exceptions import LoaderError


class TestSnowflakeLoaderEdgeCases:
    """Test edge cases and boundary conditions"""
    
    @patch('pandas.read_parquet')
    def test_load_file_with_special_characters(self, mock_read_parquet, loader, sample_data_file):
        """Test loading file with special characters in data"""
        special_char_df = pd.DataFrame({
            'VendorID': [1, 2, 3],
            'tpep_pickup_datetime': pd.to_datetime(['2024-01-01'] * 3),
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01'] * 3),
            'total_amount': [15.5, 22.3, 12.1],
            'special_field': ['café', 'naïve', 'résumé']  # Unicode characters
        })
        
        mock_read_parquet.return_value = special_char_df
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            # Should not raise encoding errors
            with patch.object(loader, 'get_connection'):
                with patch('src.loaders.snowflake_loader.write_pandas') as mock_write:
                    mock_write.return_value = (True, 1, 3, None)
                    result = loader.load_parquet_file(temp_path, "test_table", sample_data_file)
                    assert result["status"] == "completed"
        finally:
            os.unlink(temp_path)
    
    @patch('pandas.read_parquet')
    def test_load_file_with_extreme_values(self, mock_read_parquet, loader, sample_data_file):
        """Test loading file with extreme numeric values"""
        extreme_df = pd.DataFrame({
            'VendorID': [1, 2, 3],
            'tpep_pickup_datetime': pd.to_datetime(['2024-01-01'] * 3),
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01'] * 3),
            'total_amount': [0.01, 999999.99, -999999.99],  # Extreme values
            'trip_distance': [0.0, 1000000.0, np.inf],  # Including infinity
            'passenger_count': [0, 255, np.nan]  # Edge values and NaN
        })
        
        mock_read_parquet.return_value = extreme_df
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            with patch.object(loader, 'get_connection'):
                with patch('src.loaders.snowflake_loader.write_pandas') as mock_write:
                    mock_write.return_value = (True, 1, 3, None)
                    result = loader.load_parquet_file(temp_path, "test_table", sample_data_file)
                    # Should handle extreme values gracefully
                    assert result["status"] == "completed"
        finally:
            os.unlink(temp_path)
    
    @patch('pandas.read_parquet')
    def test_load_file_with_all_null_columns(self, mock_read_parquet, loader, sample_data_file):
        """Test loading file where entire columns are null"""
        all_null_df = pd.DataFrame({
            'VendorID': [None, None, None],
            'tpep_pickup_datetime': [None, None, None],
            'tpep_dropoff_datetime': [None, None, None],
            'total_amount': [None, None, None],
            'trip_distance': [15.5, 22.3, 12.1]  # Only one column has data
        })
        
        mock_read_parquet.return_value = all_null_df
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            # Should fail data quality validation
            with pytest.raises(LoaderError) as exc_info:
                loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            assert "Data quality validation failed" in str(exc_info.value)
        finally:
            os.unlink(temp_path)
    
    def test_load_file_with_single_row(self, loader, sample_data_file):
        """Test loading file with only one row of data"""
        single_row_df = pd.DataFrame({
            'VendorID': [1],
            'tpep_pickup_datetime': pd.to_datetime(['2024-01-01 10:00:00']),
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01 10:30:00']),
            'total_amount': [15.5]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
            single_row_df.to_parquet(temp_path, index=False)
        
        try:
            with patch.object(loader, 'get_connection'):
                with patch('src.loaders.snowflake_loader.write_pandas') as mock_write:
                    mock_write.return_value = (True, 1, 1, None)
                    result = loader.load_parquet_file(temp_path, "test_table", sample_data_file)
                    
                    assert result["status"] == "completed"
                    assert result["total_records"] == 1
                    assert result["loaded_records"] == 1
        finally:
            os.unlink(temp_path)
    
    @patch('pandas.read_parquet')
    def test_load_file_with_mixed_data_types(self, mock_read_parquet, loader, sample_data_file):
        """Test loading file with mixed and inconsistent data types"""
        mixed_types_df = pd.DataFrame({
            'VendorID': [1, '2', 3.0],  # Mixed int/string/float
            'tpep_pickup_datetime': [
                '2024-01-01 10:00:00',
                pd.Timestamp('2024-01-01 11:00:00'),
                '2024-01-01T12:00:00Z'  # Different timestamp formats
            ],
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01'] * 3),
            'total_amount': ['15.50', 22, None],  # Mixed string/int/null
            'boolean_field': [True, 1, 'yes']  # Mixed boolean representations
        })
        
        mock_read_parquet.return_value = mixed_types_df
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            with patch.object(loader, 'get_connection'):
                with patch('src.loaders.snowflake_loader.write_pandas') as mock_write:
                    mock_write.return_value = (True, 1, 3, None)
                    # Should handle mixed types without crashing
                    result = loader.load_parquet_file(temp_path, "test_table", sample_data_file)
                    assert result["total_records"] == 3
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderPerformanceScenarios:
    """Test performance-related scenarios"""
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_very_small_batch_size(self, mock_read_parquet, mock_write_pandas, 
                                   mock_connect, loader, sample_data_file):
        """Test performance with very small batch sizes"""
        # Create 10-row dataframe
        small_df = pd.DataFrame({
            'VendorID': list(range(1, 11)),
            'tpep_pickup_datetime': pd.date_range('2024-01-01', periods=10, freq='1H'),
            'tpep_dropoff_datetime': pd.date_range('2024-01-01 00:30:00', periods=10, freq='1H'),
            'total_amount': np.random.uniform(10, 50, 10)
        })
        
        mock_read_parquet.return_value = small_df
        mock_write_pandas.return_value = (True, 1, 1, None)  # Each batch has 1 record
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            result = loader.load_parquet_file(
                temp_path, "test_table", sample_data_file, batch_size=1
            )
            
            assert result["status"] == "completed"
            assert result["total_records"] == 10
            # Should make 10 separate calls to write_pandas
            assert mock_write_pandas.call_count == 10
        finally:
            os.unlink(temp_path)
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_very_large_batch_size(self, mock_read_parquet, mock_write_pandas, 
                                   mock_connect, loader, sample_data_file):
        """Test performance with very large batch sizes"""
        # Create 100-row dataframe
        large_df = pd.DataFrame({
            'VendorID': np.random.choice([1, 2], 100),
            'tpep_pickup_datetime': pd.date_range('2024-01-01', periods=100, freq='1H'),
            'tpep_dropoff_datetime': pd.date_range('2024-01-01 00:30:00', periods=100, freq='1H'),
            'total_amount': np.random.uniform(10, 50, 100)
        })
        
        mock_read_parquet.return_value = large_df
        mock_write_pandas.return_value = (True, 1, 100, None)  # Single large batch
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            result = loader.load_parquet_file(
                temp_path, "test_table", sample_data_file, batch_size=10000
            )
            
            assert result["status"] == "completed"
            assert result["total_records"] == 100
            # Should make only 1 call to write_pandas (entire dataset in one batch)
            assert mock_write_pandas.call_count == 1
        finally:
            os.unlink(temp_path)
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_timeout_simulation(self, mock_read_parquet, mock_write_pandas, 
                                mock_connect, loader, sample_data_file, sample_dataframe):
        """Test handling of timeouts during data loading"""
        mock_read_parquet.return_value = sample_dataframe
        
        # Simulate timeout exception
        import snowflake.connector.errors
        mock_write_pandas.side_effect = snowflake.connector.errors.DatabaseError("Timeout occurred")
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            with pytest.raises(LoaderError) as exc_info:
                loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            assert "Failed to load" in str(exc_info.value)
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderMemoryHandling:
    """Test memory-related scenarios"""
    
    @patch('pandas.read_parquet')
    def test_load_file_memory_efficient_processing(self, mock_read_parquet, loader, sample_data_file):
        """Test that large datasets don't cause memory issues"""
        # Simulate reading a large dataset
        large_dataset = pd.DataFrame({
            'VendorID': np.random.choice([1, 2], 50000),
            'tpep_pickup_datetime': pd.date_range('2024-01-01', periods=50000, freq='1min'),
            'tpep_dropoff_datetime': pd.date_range('2024-01-01 00:30:00', periods=50000, freq='1min'),
            'total_amount': np.random.uniform(5, 100, 50000),
            'trip_distance': np.random.uniform(0.1, 50, 50000)
        })
        
        mock_read_parquet.return_value = large_dataset
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            with patch.object(loader, 'get_connection'):
                with patch('src.loaders.snowflake_loader.write_pandas') as mock_write:
                    mock_write.return_value = (True, 1, 1000, None)
                    
                    # Should process in batches without memory issues
                    result = loader.load_parquet_file(
                        temp_path, "test_table", sample_data_file, batch_size=1000
                    )
                    
                    assert result["total_records"] == 50000
                    # Should make 50 batch calls (50000 / 1000)
                    assert mock_write.call_count == 50
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderDataIntegrity:
    """Test data integrity and consistency"""
    
    def test_record_hash_collision_resistance(self, loader):
        """Test that record hashes are collision-resistant"""
        # Create records that are similar but different
        record1 = {'VendorID': 1, 'total_amount': 15.50, 'trip_distance': 2.5}
        record2 = {'VendorID': 1, 'total_amount': 15.51, 'trip_distance': 2.5}  # Tiny difference
        record3 = {'VendorID': 2, 'total_amount': 15.50, 'trip_distance': 2.5}  # Different vendor
        
        hash1 = loader._calculate_record_hash(record1)
        hash2 = loader._calculate_record_hash(record2)
        hash3 = loader._calculate_record_hash(record3)
        
        # All hashes should be different
        assert hash1 != hash2
        assert hash1 != hash3
        assert hash2 != hash3
        
        # Hashes should be deterministic
        assert hash1 == loader._calculate_record_hash(record1)
    
    @patch('pandas.read_parquet')
    def test_data_consistency_across_batches(self, mock_read_parquet, loader, sample_data_file):
        """Test that data is consistent when split across batches"""
        # Create dataframe where we can track data consistency
        consistent_df = pd.DataFrame({
            'VendorID': [1] * 10,
            'tpep_pickup_datetime': pd.date_range('2024-01-01', periods=10, freq='1H'),
            'tpep_dropoff_datetime': pd.date_range('2024-01-01 00:30:00', periods=10, freq='1H'),
            'total_amount': [15.50] * 10,  # All same amount for consistency check
            'unique_id': list(range(10))  # Unique identifier
        })
        
        mock_read_parquet.return_value = consistent_df
        
        captured_batches = []
        
        def capture_batch_data(*args, **kwargs):
            captured_batches.append(kwargs['df'].copy())
            return (True, 1, len(kwargs['df']), None)
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            with patch.object(loader, 'get_connection'):
                with patch('src.loaders.snowflake_loader.write_pandas', side_effect=capture_batch_data):
                    loader.load_parquet_file(
                        temp_path, "test_table", sample_data_file, batch_size=3
                    )
            
            # Reconstruct full dataset from batches
            reconstructed_df = pd.concat(captured_batches, ignore_index=True)
            
            # Verify no data loss or corruption
            assert len(reconstructed_df) == 10
            assert set(reconstructed_df['unique_id']) == set(range(10))
            assert all(reconstructed_df['total_amount'] == 15.50)
            
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderConcurrencyScenarios:
    """Test scenarios that might occur in concurrent environments"""
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    def test_connection_pool_simulation(self, mock_connect, loader):
        """Test behavior when simulating connection pooling"""
        # Simulate multiple connection attempts
        mock_connections = [Mock() for _ in range(3)]
        mock_connect.side_effect = mock_connections
        
        # Make multiple connection context manager calls
        connections_used = []
        for i in range(3):
            with loader.get_connection() as conn:
                connections_used.append(conn)
        
        # Verify each call got a separate connection
        assert len(connections_used) == 3
        assert len(set(id(conn) for conn in connections_used)) == 3
        
        # Verify all connections were closed
        for conn in mock_connections:
            conn.close.assert_called_once()
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_resource_cleanup_on_interruption(self, mock_read_parquet, mock_write_pandas, 
                                              mock_connect, loader, sample_data_file, sample_dataframe):
        """Test that resources are cleaned up when operations are interrupted"""
        mock_read_parquet.return_value = sample_dataframe
        
        # Simulate interruption during write operation
        mock_write_pandas.side_effect = KeyboardInterrupt("User interrupted")
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            with pytest.raises(LoaderError):  # Should wrap the KeyboardInterrupt
                loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            # Connection should still be closed despite interruption
            mock_connection.close.assert_called_once()
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderConfigurationEdgeCases:
    """Test edge cases in configuration handling"""
    
    def test_loader_with_minimal_config(self, snowflake_config_minimal):
        """Test loader works with minimal configuration"""
        loader = SnowflakeLoader(snowflake_config_minimal)
        
        assert loader.config.account == "test"
        assert loader.config.username == "test"
        assert loader.config.role is None  # Should handle missing optional fields
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    def test_connection_with_missing_optional_params(self, mock_connect, snowflake_config_minimal):
        """Test connection when optional parameters are missing"""
        loader = SnowflakeLoader(snowflake_config_minimal)
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with loader.get_connection():
            pass
        
        # Verify connection was attempted with available parameters
        call_kwargs = mock_connect.call_args[1]
        assert 'account' in call_kwargs
        assert 'user' in call_kwargs
        assert 'password' in call_kwargs
        # Optional parameters should be None or missing
        assert call_kwargs.get('role') is None


if __name__ == "__main__":
    pytest.main([__file__])