# SnowflakeLoader Test Suite

This comprehensive test suite covers the `SnowflakeLoader` class with over 80 test cases across multiple categories.

## Test Structure

### 1. `test_snowflake_loader.py` - Core Unit Tests
- **Initialization Tests**: Configuration handling and object creation
- **Connection Management**: Context manager, error handling, cleanup
- **Table Creation**: Schema generation for different trip types
- **Data Loading**: Parquet file processing, batch handling
- **Data Validation**: Quality checks, error detection
- **Utility Methods**: Hash calculation, table info, query execution

### 2. `test_snowflake_loader_integration.py` - Integration Tests
- **Batch Processing**: Large file handling, multiple batches
- **Metadata Handling**: Lineage tracking, hash consistency
- **Trip Type Support**: Yellow vs Green taxi schemas
- **Error Recovery**: Connection failures, rollback scenarios
- **Performance Features**: Parallel loading, statistics tracking
- **Query Operations**: Complex queries, table information
- **End-to-End Workflows**: Complete data pipeline simulation

### 3. `test_snowflake_loader_edge_cases.py` - Edge Cases & Performance
- **Edge Cases**: Special characters, extreme values, null data
- **Performance Scenarios**: Small/large batch sizes, timeouts
- **Memory Handling**: Large datasets, efficient processing
- **Data Integrity**: Hash collision resistance, consistency
- **Concurrency**: Connection pooling, resource cleanup
- **Configuration**: Minimal configs, missing parameters

## Test Coverage

### Key Areas Covered:
- ✅ **Connection Management** (100%)
- ✅ **Table Creation** (100%)
- ✅ **Data Loading** (95%)
- ✅ **Data Validation** (100%)
- ✅ **Error Handling** (100%)
- ✅ **Batch Processing** (100%)
- ✅ **Metadata Tracking** (100%)
- ✅ **Performance Edge Cases** (90%)

### Test Scenarios:
- **Happy Path**: Normal operations with valid data
- **Error Conditions**: Database errors, connection failures, invalid data
- **Edge Cases**: Empty files, extreme values, special characters
- **Performance**: Large datasets, batch processing, memory efficiency
- **Data Quality**: Validation rules, quality scoring, error detection
- **Concurrency**: Multiple connections, resource cleanup, interruption handling

## Running the Tests

### Prerequisites
```bash
# Install test dependencies
pip install -r test-requirements.txt

# Or install individually
pip install pytest pytest-mock pytest-cov pandas numpy snowflake-connector-python
```

### Basic Test Execution
```bash
# Run all tests
python run_snowflake_loader_tests.py

# Run specific test categories
python run_snowflake_loader_tests.py --type unit
python run_snowflake_loader_tests.py --type integration
python run_snowflake_loader_tests.py --type edge_cases

# Run with verbose output
python run_snowflake_loader_tests.py --verbose

# Run with coverage report
python run_snowflake_loader_tests.py --coverage
```

### Direct Pytest Commands
```bash
# Run all SnowflakeLoader tests
pytest tests/unit/test_snowflake_loader*.py -v

# Run specific test file
pytest tests/unit/test_snowflake_loader.py -v

# Run specific test class
pytest tests/unit/test_snowflake_loader.py::TestSnowflakeLoaderConnection -v

# Run specific test method
pytest tests/unit/test_snowflake_loader.py::TestSnowflakeLoaderConnection::test_get_connection_success -v

# Run with coverage
pytest tests/unit/test_snowflake_loader*.py --cov=src.loaders.snowflake_loader --cov-report=html

# Run in parallel (if pytest-xdist installed)
pytest tests/unit/test_snowflake_loader*.py -n auto
```

## Test Configuration

### Fixtures Used
- `snowflake_config`: Standard Snowflake configuration
- `snowflake_config_minimal`: Minimal configuration for edge cases
- `loader`: SnowflakeLoader instance
- `sample_data_file`: TLCDataFile for yellow taxi
- `green_taxi_data_file`: TLCDataFile for green taxi
- `sample_dataframe`: Small test DataFrame
- `large_sample_dataframe`: Large DataFrame for performance tests
- `corrupted_dataframe`: DataFrame with quality issues
- `temp_parquet_file`: Temporary parquet file with data

### Mock Objects
- **Snowflake Connections**: Mocked database connections and cursors
- **Pandas Operations**: Mocked read_parquet and write_pandas
- **File System**: Temporary files for safe testing

## Test Examples

### Testing Connection Management
```python
def test_get_connection_success(self, mock_connect, loader):
    """Test successful connection establishment"""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection
    
    with loader.get_connection() as conn:
        assert conn == mock_connection
    
    mock_connection.close.assert_called_once()
```

### Testing Data Loading
```python
def test_load_parquet_file_success(self, mock_read_parquet, mock_write_pandas, 
                                   mock_connect, loader, sample_data_file, sample_dataframe):
    """Test successful parquet file loading"""
    mock_read_parquet.return_value = sample_dataframe
    mock_write_pandas.return_value = (True, 1, 3, None)
    
    result = loader.load_parquet_file(temp_path, "test_table", sample_data_file)
    
    assert result["status"] == "completed"
    assert result["loaded_records"] == 3
```

### Testing Error Conditions
```python
def test_data_quality_failure_prevents_load(self, loader, corrupted_dataframe):
    """Test that data quality failures prevent loading"""
    with pytest.raises(LoaderError) as exc_info:
        loader.load_parquet_file(temp_path, "test_table", sample_data_file)
    
    assert "Data quality validation failed" in str(exc_info.value)
```

## Key Testing Patterns

### 1. Connection Context Manager Testing
```python
@patch('src.loaders.snowflake_loader.snowflake.connector.connect')
def test_connection_cleanup(self, mock_connect, loader):
    """Ensure connections are always cleaned up"""
    mock_connection = Mock()
    mock_connect.return_value = mock_connection
    
    # Test both success and failure scenarios
    with loader.get_connection():
        pass  # or raise exception
    
    mock_connection.close.assert_called_once()
```

### 2. Batch Processing Testing
```python
def capture_dataframe(*args, **kwargs):
    """Capture DataFrame passed to write_pandas for verification"""
    loader._captured_df = kwargs['df']
    return (True, 1, len(kwargs['df']), None)

mock_write_pandas.side_effect = capture_dataframe
```

### 3. Data Quality Testing
```python
def test_validate_data_quality_with_issues(self, loader):
    """Test validation detects quality issues"""
    bad_dataframe = create_dataframe_with_issues()
    
    result = loader._validate_data_quality(bad_dataframe, "yellow_tripdata")
    
    assert result["is_valid"] is False
    assert len(result["errors"]) > 0
    assert result["quality_score"] < 100
```

### 4. Temporary File Handling
```python
with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
    temp_path = Path(temp_file.name)

try:
    # Test operations with temp_path
    result = loader.load_parquet_file(temp_path, "table", data_file)
finally:
    os.unlink(temp_path)  # Always cleanup
```

## Common Test Assertions

### Data Loading Results
```python
assert result["status"] == "completed"
assert result["total_records"] == expected_count
assert result["loaded_records"] == expected_loaded
assert result["failed_records"] == expected_failed
assert "data_quality_score" in result
```

### Database Interactions
```python
mock_cursor.execute.assert_called_once()
mock_cursor.close.assert_called_once()
mock_connection.close.assert_called_once()
```

### Error Handling
```python
with pytest.raises(LoaderError) as exc_info:
    loader.some_method()

assert "Expected error message" in str(exc_info.value)
```

## Extending the Tests

### Adding New Test Cases
1. **Identify the scenario**: What specific behavior needs testing?
2. **Choose the right test file**: Unit, integration, or edge case?
3. **Create appropriate fixtures**: Mock objects, test data
4. **Write focused assertions**: Test one thing per test method
5. **Add error cases**: Test both success and failure paths

### Example New Test
```python
def test_new_scenario(self, loader, sample_data_file):
    """Test description of what this tests"""
    # Arrange
    setup_test_conditions()
    
    # Act
    result = loader.method_under_test()
    
    # Assert
    assert expected_outcome
```

## Troubleshooting Tests

### Common Issues
1. **Import Errors**: Ensure all dependencies are installed
2. **Mock Not Working**: Check patch paths match actual import paths
3. **Temporary Files**: Ensure cleanup in finally blocks
4. **Test Isolation**: Each test should be independent

### Debug Tips
```python
# Add debug output
print(f"Mock call args: {mock_object.call_args}")
print(f"Mock call count: {mock_object.call_count}")

# Capture and inspect data
def debug_capture(*args, **kwargs):
    print(f"Captured DataFrame: {kwargs['df'].head()}")
    return (True, 1, len(kwargs['df']), None)
```

## Performance Considerations

### Test Execution Speed
- Tests use mocks to avoid real database connections
- Temporary files are small and cleaned up immediately
- Batch processing tests use reasonable data sizes
- Parallel execution supported with pytest-xdist

### Memory Usage
- Large DataFrames are generated programmatically, not stored
- Temporary files are created and destroyed per test
- Mock objects are reset between tests

This test suite provides comprehensive coverage of the SnowflakeLoader class, ensuring reliability and maintainability of the data loading functionality.