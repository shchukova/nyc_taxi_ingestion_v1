# tests/unit/test_snowflake_loader_integration.py
"""
Integration tests for SnowflakeLoader class
These tests focus on testing multiple components working together
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, call
import tempfile
import os

from src.loaders.snowflake_loader import SnowflakeLoader
from src.utils.exceptions import LoaderError


class TestSnowflakeLoaderBatchProcessing:
    """Test batch processing capabilities"""
    
    @patch('snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_large_file_batch_processing(self, mock_read_parquet, mock_write_pandas, 
                                         mock_connect, loader, sample_data_file, large_sample_dataframe):
        """Test loading large file with multiple batches"""
        mock_read_parquet.return_value = large_sample_dataframe
        mock_write_pandas.return_value = (True, 1, 100, None)  # Each batch succeeds
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            result = loader.load_parquet_file(
                temp_path, "test_table", sample_data_file, batch_size=100
            )
            
            assert result["status"] == "completed"
            assert result["total_records"] == 1000
            assert result["loaded_records"] == 1000
            assert result["failed_records"] == 0
            
            # Verify write_pandas was called 10 times (1000 records / 100 batch size)
            assert mock_write_pandas.call_count == 10
            
        finally:
            os.unlink(temp_path)
    
    @patch('snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_batch_processing_with_failures(self, mock_read_parquet, mock_write_pandas, 
                                            mock_connect, loader, sample_data_file, large_sample_dataframe):
        """Test batch processing where some batches fail"""
        mock_read_parquet.return_value = large_sample_dataframe
        
        # First 5 batches succeed, next 3 fail, last 2 succeed
        mock_write_pandas.side_effect = [
            (True, 1, 100, None),   # Batch 1: Success
            (True, 1, 100, None),   # Batch 2: Success
            (True, 1, 100, None),   # Batch 3: Success
            (True, 1, 100, None),   # Batch 4: Success
            (True, 1, 100, None),   # Batch 5: Success
            (False, 0, 0, None),    # Batch 6: Fail
            (False, 0, 0, None),    # Batch 7: Fail
            (False, 0, 0, None),    # Batch 8: Fail
            (True, 1, 100, None),   # Batch 9: Success
            (True, 1, 100, None),   # Batch 10: Success
        ]
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            result = loader.load_parquet_file(
                temp_path, "test_table", sample_data_file, batch_size=100
            )
            
            assert result["status"] == "partial"
            assert result["total_records"] == 1000
            assert result["loaded_records"] == 700  # 7 successful batches
            assert result["failed_records"] == 300  # 3 failed batches
            
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderMetadataHandling:
    """Test metadata and lineage tracking"""
    
    @patch('snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_metadata_columns_added(self, mock_read_parquet, mock_write_pandas, 
                                    mock_connect, loader, sample_data_file, sample_dataframe):
        """Test that metadata columns are properly added to data"""
        mock_read_parquet.return_value = sample_dataframe
        mock_write_pandas.return_value = (True, 1, 3, None)
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        # Capture the DataFrame passed to write_pandas
        def capture_dataframe(*args, **kwargs):
            loader._captured_df = kwargs['df']
            return (True, 1, len(kwargs['df']), None)
        
        mock_write_pandas.side_effect = capture_dataframe
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            # Check that metadata columns were added
            captured_df = loader._captured_df
            assert '_file_name' in captured_df.columns
            assert '_load_timestamp' in captured_df.columns
            assert '_record_hash' in captured_df.columns
            
            # Check metadata values
            assert all(captured_df['_file_name'] == sample_data_file.filename)
            assert captured_df['_record_hash'].nunique() == len(captured_df)  # All hashes should be unique
            
        finally:
            os.unlink(temp_path)
    
    @patch('snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_record_hash_consistency(self, mock_read_parquet, mock_write_pandas, 
                                     mock_connect, loader, sample_data_file):
        """Test that identical records produce identical hashes"""
        # Create DataFrame with duplicate records
        duplicate_dataframe = pd.DataFrame({
            'VendorID': [1, 1, 2],  # First two rows are identical
            'tpep_pickup_datetime': pd.to_datetime(['2024-01-01 10:00:00'] * 3),
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01 10:30:00'] * 3),
            'total_amount': [15.5, 15.5, 22.3]  # First two amounts are identical
        })
        
        mock_read_parquet.return_value = duplicate_dataframe
        
        def capture_dataframe(*args, **kwargs):
            loader._captured_df = kwargs['df']
            return (True, 1, len(kwargs['df']), None)
        
        mock_write_pandas.side_effect = capture_dataframe
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            captured_df = loader._captured_df
            hashes = captured_df['_record_hash'].tolist()
            
            # First two rows should have identical hashes (same data)
            assert hashes[0] == hashes[1]
            # Third row should have different hash
            assert hashes[0] != hashes[2]
            
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderDifferentTripTypes:
    """Test handling of different trip types"""
    
    @patch('snowflake.connector.connect')
    def test_create_yellow_vs_green_tables(self, mock_connect, loader):
        """Test that different schemas are created for different trip types"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Create yellow taxi table
        loader.create_raw_table("yellow_table", "yellow_tripdata")
        yellow_sql = mock_cursor.execute.call_args_list[0][0][0]
        
        # Reset mock
        mock_cursor.reset_mock()
        
        # Create green taxi table
        loader.create_raw_table("green_table", "green_tripdata")
        green_sql = mock_cursor.execute.call_args_list[0][0][0]
        
        # Verify different schemas
        assert "tpep_pickup_datetime" in yellow_sql
        assert "tpep_pickup_datetime" not in green_sql
        assert "lpep_pickup_datetime" in green_sql
        assert "lpep_pickup_datetime" not in yellow_sql
        assert "trip_type INTEGER" in green_sql
        assert "trip_type INTEGER" not in yellow_sql
    
    @patch('snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_load_green_taxi_data(self, mock_read_parquet, mock_write_pandas, 
                                  mock_connect, loader, green_taxi_data_file, green_taxi_dataframe):
        """Test loading green taxi data with proper validation"""
        mock_read_parquet.return_value = green_taxi_dataframe
        mock_write_pandas.return_value = (True, 1, 4, None)
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            result = loader.load_parquet_file(temp_path, "green_table", green_taxi_data_file)
            
            assert result["status"] == "completed"
            assert result["loaded_records"] == 4
            
            # Verify data quality validation used green taxi logic
            assert "data_quality_score" in result
            assert result["data_quality_score"] > 0
            
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderErrorRecovery:
    """Test error recovery and rollback scenarios"""
    
    @patch('snowflake.connector.connect')
    @patch('pandas.read_parquet')
    def test_connection_failure_during_load(self, mock_read_parquet, mock_connect, 
                                            loader, sample_data_file, sample_dataframe):
        """Test handling of connection failures during data loading"""
        mock_read_parquet.return_value = sample_dataframe
        
        # Connection fails when trying to load data
        import snowflake.connector.errors
        mock_connect.side_effect = snowflake.connector.errors.DatabaseError("Connection lost")
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            with pytest.raises(LoaderError) as exc_info:
                loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            assert "Snowflake connection failed" in str(exc_info.value)
            
        finally:
            os.unlink(temp_path)
    
    @patch('snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_data_quality_failure_prevents_load(self, mock_read_parquet, mock_write_pandas, 
                                                 mock_connect, loader, sample_data_file, corrupted_dataframe):
        """Test that data quality failures prevent loading"""
        mock_read_parquet.return_value = corrupted_dataframe
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            with pytest.raises(LoaderError) as exc_info:
                loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            assert "Data quality validation failed" in str(exc_info.value)
            # Ensure write_pandas was never called due to validation failure
            mock_write_pandas.assert_not_called()
            
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderPerformanceFeatures:
    """Test performance-related features"""
    
    @patch('snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_parallel_loading_configuration(self, mock_read_parquet, mock_write_pandas, 
                                            mock_connect, loader, sample_data_file, sample_dataframe):
        """Test that parallel loading is properly configured"""
        mock_read_parquet.return_value = sample_dataframe
        mock_write_pandas.return_value = (True, 1, 3, None)
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            # Verify write_pandas was called with parallel configuration
            call_kwargs = mock_write_pandas.call_args[1]
            assert call_kwargs['parallel'] == 4
            assert call_kwargs['compression'] == 'gzip'
            assert call_kwargs['on_error'] == 'continue'
            
        finally:
            os.unlink(temp_path)
    
    @patch('snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_load_statistics_tracking(self, mock_read_parquet, mock_write_pandas, 
                                      mock_connect, loader, sample_data_file, sample_dataframe):
        """Test comprehensive load statistics are tracked"""
        mock_read_parquet.return_value = sample_dataframe
        mock_write_pandas.return_value = (True, 1, 3, None)
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            result = loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            # Verify all required statistics are present
            required_keys = [
                "status", "total_records", "loaded_records", "failed_records",
                "file_path", "table_name", "load_timestamp", "data_quality_score"
            ]
            
            for key in required_keys:
                assert key in result, f"Missing key: {key}"
            
            assert result["file_path"] == str(temp_path)
            assert isinstance(result["load_timestamp"], str)
            assert 0 <= result["data_quality_score"] <= 100
            
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderQueryOperations:
    """Test query execution and table information operations"""
    
    @patch('snowflake.connector.connect')
    def test_table_info_with_real_data(self, mock_connect, loader):
        """Test getting table information with realistic data"""
        mock_connection = Mock()
        mock_cursor = Mock()
        
        # Mock realistic table info response
        mock_cursor.fetchone.return_value = (
            50000,  # row_count
            '2024-01-01 10:00:00',  # first_load
            '2024-01-31 23:59:59',  # last_load
            31  # unique_files (one per day for January)
        )
        
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        result = loader.get_table_info("yellow_tripdata_raw")
        
        assert result["table_name"] == "yellow_tripdata_raw"
        assert result["row_count"] == 50000
        assert result["first_load"] == '2024-01-01 10:00:00'
        assert result["last_load"] == '2024-01-31 23:59:59'
        assert result["unique_files"] == 31
    
    @patch('snowflake.connector.connect')
    def test_execute_complex_query(self, mock_connect, loader):
        """Test executing complex analytical queries"""
        mock_connection = Mock()
        mock_cursor = Mock()
        
        # Mock complex query results
        mock_cursor.description = [('date',), ('total_trips',), ('avg_fare',), ('max_distance',)]
        mock_cursor.fetchall.return_value = [
            ('2024-01-01', 1500, 18.50, 45.2),
            ('2024-01-02', 1723, 19.20, 52.1),
            ('2024-01-03', 1456, 17.80, 38.9)
        ]
        
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        query = """
        SELECT 
            DATE(tpep_pickup_datetime) as date,
            COUNT(*) as total_trips,
            AVG(total_amount) as avg_fare,
            MAX(trip_distance) as max_distance
        FROM yellow_tripdata_raw 
        WHERE tpep_pickup_datetime >= '2024-01-01'
        GROUP BY DATE(tpep_pickup_datetime)
        ORDER BY date
        """
        
        result = loader.execute_query(query)
        
        assert len(result) == 3
        assert result[0]['date'] == '2024-01-01'
        assert result[0]['total_trips'] == 1500
        assert result[1]['avg_fare'] == 19.20
        assert result[2]['max_distance'] == 38.9


class TestSnowflakeLoaderEndToEnd:
    """End-to-end integration tests"""
    
    @patch('snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_complete_workflow_yellow_taxi(self, mock_read_parquet, mock_write_pandas, 
                                           mock_connect, loader, sample_data_file, sample_dataframe):
        """Test complete workflow: create table -> load data -> query info"""
        # Setup mocks
        mock_read_parquet.return_value = sample_dataframe
        mock_write_pandas.return_value = (True, 1, 3, None)
        
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Mock table info query response
        mock_cursor.fetchone.return_value = (3, '2024-01-01', '2024-01-01', 1)
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            # 1. Create table
            table_created = loader.create_raw_table("test_yellow_table", "yellow_tripdata")
            assert table_created is True
            
            # 2. Load data
            load_result = loader.load_parquet_file(temp_path, "test_yellow_table", sample_data_file)
            assert load_result["status"] == "completed"
            assert load_result["loaded_records"] == 3
            
            # 3. Get table info
            table_info = loader.get_table_info("test_yellow_table")
            assert table_info["row_count"] == 3
            assert table_info["unique_files"] == 1
            
            # Verify all operations called the database
            assert mock_cursor.execute.call_count >= 2  # At least table creation + info query
            
        finally:
            os.unlink(temp_path)
    
    @patch('snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_multi_file_loading_simulation(self, mock_read_parquet, mock_write_pandas, 
                                           mock_connect, loader, sample_dataframe):
        """Simulate loading multiple files with different characteristics"""
        mock_write_pandas.return_value = (True, 1, 3, None)
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Create multiple data files
        files_to_load = [
            TLCDataFile("yellow_tripdata", 2024, 1, "https://example.com/jan.parquet", 
                       "yellow_tripdata_2024-01.parquet", 100),
            TLCDataFile("yellow_tripdata", 2024, 2, "https://example.com/feb.parquet", 
                       "yellow_tripdata_2024-02.parquet", 95),
            TLCDataFile("yellow_tripdata", 2024, 3, "https://example.com/mar.parquet", 
                       "yellow_tripdata_2024-03.parquet", 105)
        ]
        
        # Load each file
        load_results = []
        for data_file in files_to_load:
            mock_read_parquet.return_value = sample_dataframe
            
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                temp_path = Path(temp_file.name)
            
            try:
                result = loader.load_parquet_file(temp_path, "yellow_tripdata_raw", data_file)
                load_results.append(result)
            finally:
                os.unlink(temp_path)
        
        # Verify all files were loaded successfully
        assert len(load_results) == 3
        for result in load_results:
            assert result["status"] == "completed"
            assert result["loaded_records"] == 3
        
        # Verify write_pandas was called for each file
        assert mock_write_pandas.call_count == 3


if __name__ == "__main__":
    pytest.main([__file__])