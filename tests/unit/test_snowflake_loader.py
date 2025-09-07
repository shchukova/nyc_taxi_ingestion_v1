# tests/unit/test_snowflake_loader.py
"""
Unit tests for SnowflakeLoader class
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from contextlib import contextmanager
import tempfile
import os

from src.loaders.snowflake_loader import SnowflakeLoader
from src.config.settings import SnowflakeConfig
from src.data_sources.tlc_data_source import TLCDataFile
from src.utils.exceptions import LoaderError


class TestSnowflakeLoader:
    """Test suite for SnowflakeLoader class"""
    
    @pytest.fixture
    def snowflake_config(self):
        """Create a test Snowflake configuration"""
        return SnowflakeConfig(
            account="test_account",
            username="test_user",
            password="test_password",
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            role="test_role"
        )
    
    @pytest.fixture
    def loader(self, snowflake_config):
        """Create SnowflakeLoader instance"""
        return SnowflakeLoader(snowflake_config)
    
    @pytest.fixture
    def sample_data_file(self):
        """Create sample TLCDataFile for testing"""
        return TLCDataFile(
            trip_type="yellow_tripdata",
            year=2024,
            month=1,
            url="https://example.com/test.parquet",
            filename="yellow_tripdata_2024-01.parquet",
            estimated_size_mb=100
        )
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create sample DataFrame for testing"""
        return pd.DataFrame({
            'VendorID': [1, 2, 1],
            'tpep_pickup_datetime': pd.to_datetime(['2024-01-01 10:00:00', '2024-01-01 11:00:00', '2024-01-01 12:00:00']),
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01 10:30:00', '2024-01-01 11:30:00', '2024-01-01 12:30:00']),
            'passenger_count': [1.0, 2.0, 1.0],
            'trip_distance': [2.5, 3.2, 1.8],
            'total_amount': [15.5, 22.3, 12.1]
        })


class TestSnowflakeLoaderInitialization:
    """Test SnowflakeLoader initialization"""
    
    def test_initialization_with_config(self, snowflake_config):
        """Test loader initializes correctly with config"""
        loader = SnowflakeLoader(snowflake_config)
        
        assert loader.config == snowflake_config
        assert loader.logger is not None
        assert loader._connection is None
    
    def test_initialization_stores_config_correctly(self, snowflake_config):
        """Test that all config parameters are stored correctly"""
        loader = SnowflakeLoader(snowflake_config)
        
        assert loader.config.account == "test_account"
        assert loader.config.username == "test_user"
        assert loader.config.database == "test_database"


class TestSnowflakeLoaderConnection:
    """Test Snowflake connection management"""
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    def test_get_connection_success(self, mock_connect, loader):
        """Test successful connection establishment"""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with loader.get_connection() as conn:
            assert conn == mock_connection
        
        mock_connect.assert_called_once_with(
            account=loader.config.account,
            user=loader.config.username,
            password=loader.config.password,
            warehouse=loader.config.warehouse,
            database=loader.config.database,
            schema=loader.config.schema,
            role=loader.config.role
        )
        mock_connection.close.assert_called_once()
    
    @patch('loaders.snowflake_loader.snowflake.connector.connect')
    def test_get_connection_failure(self, mock_connect, loader):
        """Test connection failure handling"""
        import snowflake.connector.errors
        
        mock_connect.side_effect = snowflake.connector.errors.DatabaseError("Connection failed")
        
        with pytest.raises(LoaderError) as exc_info:
            with loader.get_connection():
                pass
        
        assert "Snowflake connection failed" in str(exc_info.value)
        assert "Connection failed" in str(exc_info.value)
    
    @patch('loaders.snowflake_loader.snowflake.connector.connect')
    def test_get_connection_cleanup_on_exception(self, mock_connect, loader):
        """Test connection is properly closed even when exception occurs"""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with pytest.raises(RuntimeError):
            with loader.get_connection() as conn:
                raise RuntimeError("Test exception")
        
        mock_connection.close.assert_called_once()


class TestSnowflakeLoaderTableCreation:
    """Test table creation functionality"""
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    def test_create_raw_table_yellow_success(self, mock_connect, loader):
        """Test successful yellow taxi table creation"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        result = loader.create_raw_table("test_yellow_table", "yellow_tripdata")
        
        assert result is True
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
        
        # Verify the SQL contains expected elements
        sql_call = mock_cursor.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS test_yellow_table" in sql_call
        assert "VendorID INTEGER" in sql_call
        assert "tpep_pickup_datetime TIMESTAMP" in sql_call
        assert "_file_name VARCHAR(255)" in sql_call
        assert "_load_timestamp TIMESTAMP" in sql_call
        assert "_record_hash VARCHAR(64)" in sql_call
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    def test_create_raw_table_green_success(self, mock_connect, loader):
        """Test successful green taxi table creation"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        result = loader.create_raw_table("test_green_table", "green_tripdata")
        
        assert result is True
        mock_cursor.execute.assert_called_once()
        
        # Verify the SQL contains green taxi specific elements
        sql_call = mock_cursor.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS test_green_table" in sql_call
        assert "lpep_pickup_datetime TIMESTAMP" in sql_call
        assert "trip_type INTEGER" in sql_call
    
    def test_create_raw_table_unsupported_trip_type(self, loader):
        """Test error handling for unsupported trip type"""
        with pytest.raises(LoaderError) as exc_info:
            loader.create_raw_table("test_table", "invalid_tripdata")
        
        assert "Unsupported trip type" in str(exc_info.value)
        assert "invalid_tripdata" in str(exc_info.value)
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    def test_create_raw_table_database_error(self, mock_connect, loader):
        """Test table creation with database error"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database error")
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        with pytest.raises(LoaderError) as exc_info:
            loader.create_raw_table("test_table", "yellow_tripdata")
        
        assert "Failed to create table test_table" in str(exc_info.value)
        assert "Database error" in str(exc_info.value)


class TestSnowflakeLoaderDataLoading:
    """Test data loading functionality"""
    
    def test_load_parquet_file_nonexistent_file(self, loader, sample_data_file):
        """Test loading with nonexistent file"""
        nonexistent_path = Path("/nonexistent/file.parquet")
        
        with pytest.raises(LoaderError) as exc_info:
            loader.load_parquet_file(nonexistent_path, "test_table", sample_data_file)
        
        assert "File does not exist" in str(exc_info.value)
    
    @patch('pandas.read_parquet')
    def test_load_parquet_file_empty_dataframe(self, mock_read_parquet, loader, sample_data_file):
        """Test loading with empty DataFrame"""
        mock_read_parquet.return_value = pd.DataFrame()
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            result = loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            assert result["status"] == "skipped"
            assert result["records_processed"] == 0
        finally:
            os.unlink(temp_path)
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_load_parquet_file_success(self, mock_read_parquet, mock_write_pandas, 
                                       mock_connect, loader, sample_data_file, sample_dataframe):
        """Test successful parquet file loading"""
        # Setup mocks
        mock_read_parquet.return_value = sample_dataframe
        mock_write_pandas.return_value = (True, 1, 3, None)  # success, nchunks, nrows, output
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            result = loader.load_parquet_file(temp_path, "test_table", sample_data_file, batch_size=5)
            
            assert result["status"] == "completed"
            assert result["total_records"] == 3
            assert result["loaded_records"] == 3
            assert result["failed_records"] == 0
            assert result["table_name"] == "test_table"
            
            # Verify write_pandas was called
            mock_write_pandas.assert_called_once()
            call_args = mock_write_pandas.call_args
            assert call_args[1]["table_name"] == "TEST_TABLE"  # Should be uppercase
            assert call_args[1]["chunk_size"] == 5
            
        finally:
            os.unlink(temp_path)
    
    @patch('src.loaders.snowflake_loader.snowflake.connector.connect')
    @patch('src.loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_load_parquet_file_partial_failure(self, mock_read_parquet, mock_write_pandas, 
                                               mock_connect, loader, sample_data_file, sample_dataframe):
        """Test parquet file loading with partial failures"""
        # Setup mocks
        mock_read_parquet.return_value = sample_dataframe
        mock_write_pandas.side_effect = [
            (True, 1, 2, None),   # First batch succeeds
            (False, 0, 0, None)   # Second batch fails
        ]
        
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            result = loader.load_parquet_file(temp_path, "test_table", sample_data_file, batch_size=2)
            
            assert result["status"] == "partial"
            assert result["total_records"] == 3
            assert result["loaded_records"] == 2
            assert result["failed_records"] == 1
            
        finally:
            os.unlink(temp_path)
    
    @patch('pandas.read_parquet')
    def test_load_parquet_file_data_quality_failure(self, mock_read_parquet, loader, sample_data_file):
        """Test loading with data quality validation failure"""
        # Create DataFrame with quality issues (all null critical columns)
        bad_dataframe = pd.DataFrame({
            'VendorID': [1, 2, 3],
            'tpep_pickup_datetime': [None, None, None],
            'tpep_dropoff_datetime': [None, None, None],
            'total_amount': [None, None, None]
        })
        mock_read_parquet.return_value = bad_dataframe
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            with pytest.raises(LoaderError) as exc_info:
                loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            assert "Data quality validation failed" in str(exc_info.value)
        finally:
            os.unlink(temp_path)


class TestSnowflakeLoaderDataValidation:
    """Test data quality validation functionality"""
    
    def test_validate_data_quality_good_data(self, loader, sample_dataframe):
        """Test validation with good quality data"""
        result = loader._validate_data_quality(sample_dataframe, "yellow_tripdata")
        
        assert result["is_valid"] is True
        assert result["quality_score"] == 100
        assert len(result["errors"]) == 0
        assert result["total_records"] == 3
    
    def test_validate_data_quality_high_null_percentage(self, loader):
        """Test validation with high null percentage (error condition)"""
        bad_dataframe = pd.DataFrame({
            'VendorID': [1, 2, 3, 4, 5],
            'tpep_pickup_datetime': [None, None, None, None, '2024-01-01'],  # 80% nulls
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01'] * 5),
            'total_amount': [10, 20, 30, 40, 50]
        })
        
        result = loader._validate_data_quality(bad_dataframe, "yellow_tripdata")
        
        assert result["is_valid"] is False
        assert result["quality_score"] < 100
        assert len(result["errors"]) > 0
        assert "tpep_pickup_datetime has 80.0% null values" in result["errors"]
    
    def test_validate_data_quality_medium_null_percentage(self, loader):
        """Test validation with medium null percentage (warning condition)"""
        warning_dataframe = pd.DataFrame({
            'VendorID': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'tpep_pickup_datetime': [None] * 6 + ['2024-01-01'] * 4,  # 60% nulls -> warning
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01'] * 10),
            'total_amount': list(range(10, 20))
        })
        
        result = loader._validate_data_quality(warning_dataframe, "yellow_tripdata")
        
        assert result["is_valid"] is True  # Warnings don't make it invalid
        assert result["quality_score"] < 100
        assert len(result["warnings"]) > 0
    
    def test_validate_data_quality_negative_amounts(self, loader):
        """Test validation with negative total amounts"""
        negative_dataframe = pd.DataFrame({
            'VendorID': [1, 2, 3],
            'tpep_pickup_datetime': pd.to_datetime(['2024-01-01'] * 3),
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01'] * 3),
            'total_amount': [-10, 20, 30]  # One negative amount
        })
        
        result = loader._validate_data_quality(negative_dataframe, "yellow_tripdata")
        
        assert result["is_valid"] is True  # Warnings don't invalidate
        assert any("negative total_amount" in warning for warning in result["warnings"])
    
    def test_validate_data_quality_extreme_amounts(self, loader):
        """Test validation with extreme total amounts"""
        extreme_dataframe = pd.DataFrame({
            'VendorID': [1] * 100,
            'tpep_pickup_datetime': pd.to_datetime(['2024-01-01'] * 100),
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01'] * 100),
            'total_amount': [1500] * 5 + [20] * 95  # 5% extreme amounts (>$1000)
        })
        
        result = loader._validate_data_quality(extreme_dataframe, "yellow_tripdata")
        
        assert result["is_valid"] is True
        assert any("extreme total_amount" in warning for warning in result["warnings"])
    
    def test_validate_data_quality_invalid_trip_times(self, loader):
        """Test validation with pickup after dropoff"""
        invalid_time_dataframe = pd.DataFrame({
            'VendorID': [1, 2, 3],
            'tpep_pickup_datetime': pd.to_datetime(['2024-01-01 12:00:00', '2024-01-01 11:00:00', '2024-01-01 10:00:00']),
            'tpep_dropoff_datetime': pd.to_datetime(['2024-01-01 11:30:00', '2024-01-01 10:30:00', '2024-01-01 10:30:00']),  # First two have pickup after dropoff
            'total_amount': [15, 20, 25]
        })
        
        result = loader._validate_data_quality(invalid_time_dataframe, "yellow_tripdata")
        
        assert result["is_valid"] is True  # Warnings don't invalidate
        assert any("pickup after dropoff" in warning for warning in result["warnings"])
    
    def test_validate_data_quality_green_tripdata(self, loader):
        """Test validation with green taxi data (different column names)"""
        green_dataframe = pd.DataFrame({
            'VendorID': [1, 2, 3],
            'lpep_pickup_datetime': pd.to_datetime(['2024-01-01'] * 3),
            'lpep_dropoff_datetime': pd.to_datetime(['2024-01-01'] * 3),
            'total_amount': [15, 20, 25]
        })
        
        result = loader._validate_data_quality(green_dataframe, "green_tripdata")
        
        assert result["is_valid"] is True
        assert result["quality_score"] == 100


class TestSnowflakeLoaderUtilities:
    """Test utility methods"""
    
    def test_calculate_record_hash(self, loader):
        """Test record hash calculation"""
        record1 = {'VendorID': 1, 'total_amount': 15.5}
        record2 = {'total_amount': 15.5, 'VendorID': 1}  # Same data, different order
        record3 = {'VendorID': 2, 'total_amount': 15.5}  # Different data
        
        hash1 = loader._calculate_record_hash(record1)
        hash2 = loader._calculate_record_hash(record2)
        hash3 = loader._calculate_record_hash(record3)
        
        assert hash1 == hash2  # Same data should produce same hash
        assert hash1 != hash3  # Different data should produce different hash
        assert len(hash1) == 32  # MD5 hash length
    
    def test_calculate_record_hash_with_complex_data(self, loader):
        """Test record hash with complex data types"""
        import datetime
        
        record = {
            'VendorID': 1,
            'pickup_datetime': datetime.datetime(2024, 1, 1, 10, 0, 0),
            'total_amount': 15.5,
            'passenger_count': None
        }
        
        hash_result = loader._calculate_record_hash(record)
        assert isinstance(hash_result, str)
        assert len(hash_result) == 32
    
    @patch('loaders.snowflake_loader.snowflake.connector.connect')
    def test_get_table_info_success(self, mock_connect, loader):
        """Test getting table information successfully"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1000, '2024-01-01', '2024-01-02', 5)
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        result = loader.get_table_info("test_table")
        
        assert result["table_name"] == "test_table"
        assert result["row_count"] == 1000
        assert result["first_load"] == '2024-01-01'
        assert result["last_load"] == '2024-01-02'
        assert result["unique_files"] == 5
        
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('loaders.snowflake_loader.snowflake.connector.connect')
    def test_get_table_info_database_error(self, mock_connect, loader):
        """Test getting table information with database error"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database error")
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        result = loader.get_table_info("test_table")
        
        assert "error" in result
        assert "Database error" in result["error"]
    
    @patch('loaders.snowflake_loader.snowflake.connector.connect')
    def test_execute_query_success(self, mock_connect, loader):
        """Test successful query execution"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.description = [('col1',), ('col2',), ('col3',)]
        mock_cursor.fetchall.return_value = [
            ('value1', 'value2', 'value3'),
            ('value4', 'value5', 'value6')
        ]
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        result = loader.execute_query("SELECT * FROM test_table")
        
        expected = [
            {'col1': 'value1', 'col2': 'value2', 'col3': 'value3'},
            {'col1': 'value4', 'col2': 'value5', 'col3': 'value6'}
        ]
        assert result == expected
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
        mock_cursor.close.assert_called_once()
    
    @patch('loaders.snowflake_loader.snowflake.connector.connect')
    def test_execute_query_database_error(self, mock_connect, loader):
        """Test query execution with database error"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Query failed")
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        with pytest.raises(LoaderError) as exc_info:
            loader.execute_query("SELECT * FROM test_table")
        
        assert "Query execution failed" in str(exc_info.value)
        assert "Query failed" in str(exc_info.value)


class TestSnowflakeLoaderSchemas:
    """Test schema definition methods"""
    
    def test_get_yellow_taxi_schema(self, loader):
        """Test yellow taxi schema definition"""
        schema = loader._get_yellow_taxi_schema()
        
        # Check for key yellow taxi columns
        assert "VendorID INTEGER" in schema
        assert "tpep_pickup_datetime TIMESTAMP" in schema
        assert "tpep_dropoff_datetime TIMESTAMP" in schema
        assert "passenger_count FLOAT" in schema
        assert "trip_distance FLOAT" in schema
        assert "total_amount FLOAT" in schema
        assert "congestion_surcharge FLOAT" in schema
    
    def test_get_green_taxi_schema(self, loader):
        """Test green taxi schema definition"""
        schema = loader._get_green_taxi_schema()
        
        # Check for key green taxi columns
        assert "VendorID INTEGER" in schema
        assert "lpep_pickup_datetime TIMESTAMP" in schema
        assert "lpep_dropoff_datetime TIMESTAMP" in schema
        assert "passenger_count FLOAT" in schema
        assert "trip_distance FLOAT" in schema
        assert "total_amount FLOAT" in schema
        assert "trip_type INTEGER" in schema
        assert "ehail_fee FLOAT" in schema  # Green taxi specific


class TestSnowflakeLoaderIntegration:
    """Integration-style tests for SnowflakeLoader"""
    
    @patch('loaders.snowflake_loader.snowflake.connector.connect')
    @patch('loaders.snowflake_loader.write_pandas')
    @patch('pandas.read_parquet')
    def test_full_load_workflow(self, mock_read_parquet, mock_write_pandas, 
                                mock_connect, loader, sample_data_file, sample_dataframe):
        """Test complete workflow from file loading to database insertion"""
        # Setup mocks
        mock_read_parquet.return_value = sample_dataframe
        mock_write_pandas.return_value = (True, 1, 3, None)
        
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_path = Path(temp_file.name)
        
        try:
            # Create table first
            table_created = loader.create_raw_table("test_table", "yellow_tripdata")
            assert table_created is True
            
            # Load data
            result = loader.load_parquet_file(temp_path, "test_table", sample_data_file)
            
            assert result["status"] == "completed"
            assert result["loaded_records"] == 3
            
            # Verify both table creation and data loading occurred
            assert mock_cursor.execute.call_count >= 1  # At least table creation
            mock_write_pandas.assert_called_once()
            
        finally:
            os.unlink(temp_path)
    
    @patch('loaders.snowflake_loader.snowflake.connector.connect')
    def test_error_handling_chain(self, mock_connect, loader, sample_data_file):
        """Test error handling propagates correctly through the chain"""
        # Mock connection failure
        import snowflake.connector.errors
        mock_connect.side_effect = snowflake.connector.errors.DatabaseError("Connection failed")
        
        # Test table creation fails
        with pytest.raises(LoaderError) as exc_info:
            loader.create_raw_table("test_table", "yellow_tripdata")
        assert "Snowflake connection failed" in str(exc_info.value)
        
        # Test data loading fails (with nonexistent file)
        nonexistent_path = Path("/nonexistent/file.parquet")
        with pytest.raises(LoaderError) as exc_info:
            loader.load_parquet_file(nonexistent_path, "test_table", sample_data_file)
        assert "File does not exist" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__])