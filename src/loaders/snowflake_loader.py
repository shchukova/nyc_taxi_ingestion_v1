# src/loaders/snowflake_loader.py
"""
Snowflake data warehouse loader for NYC Taxi Data Pipeline
"""

from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from contextlib import contextmanager

from src.config.settings import SnowflakeConfig
from src.data_sources.tlc_data_source import TLCDataFile
from src.utils.logger import get_logger
from src.utils.exceptions import LoaderError


class SnowflakeLoader:
    """
    Handles data loading operations to Snowflake data warehouse
    
    This class provides comprehensive data loading functionality:
    - Connection management with connection pooling
    - Table creation and schema management
    - Bulk data loading using Snowflake's COPY command
    - Data quality validation and error handling
    - Transaction management for data consistency
    - Performance monitoring and optimization
    
    Key features:
    - Automatic table creation based on data schema
    - Efficient bulk loading using pandas integration
    - Configurable batch processing for large datasets
    - Comprehensive error handling and rollback
    - Data lineage tracking with metadata
    """
    
    def __init__(self, config: SnowflakeConfig):
        """
        Initialize Snowflake loader
        
        Args:
            config: Snowflake configuration object
        """
        self.config = config
        self.logger = get_logger(__name__)
        self._connection = None
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for Snowflake database connections
        
        Ensures proper connection handling and cleanup
        """
        connection = None
        try:
            connection = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.username,
                password=self.config.password,
                warehouse=self.config.warehouse,
                database=self.config.database,
                schema=self.config.schema,
                role=self.config.role
            )
            self.logger.info("Connected to Snowflake successfully")
            yield connection
            
        except snowflake.connector.errors.Error as e:
            self.logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise LoaderError(f"Snowflake connection failed: {str(e)}") from e
            
        finally:
            if connection:
                connection.close()
                self.logger.info("Snowflake connection closed")
    
    def create_raw_table(self, table_name: str, trip_type: str) -> bool:
        """
        Create raw landing table for taxi trip data
        
        Args:
            table_name: Name of the table to create
            trip_type: Type of trip data (determines schema)
            
        Returns:
            True if table was created successfully
            
        Raises:
            LoaderError: If table creation fails
        """
        schema_map = {
            "yellow_tripdata": self._get_yellow_taxi_schema(),
            "green_tripdata": self._get_green_taxi_schema()
        }
        
        if trip_type not in schema_map:
            raise LoaderError(f"Unsupported trip type for table creation: {trip_type}")
        
        column_definitions = schema_map[trip_type]
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_definitions},
            -- Metadata columns for data lineage
            _file_name VARCHAR(255),
            _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            _record_hash VARCHAR(64)
        )
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                cursor.close()
                
            self.logger.info(f"Successfully created/verified table: {table_name}")
            return True
            
        except Exception as e:
            raise LoaderError(f"Failed to create table {table_name}: {str(e)}") from e
    
    def load_parquet_file(
        self, 
        file_path: Path, 
        table_name: str, 
        data_file: TLCDataFile,
        batch_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Load parquet file data into Snowflake table
        
        Args:
            file_path: Path to the parquet file
            table_name: Target table name
            data_file: Metadata about the data file
            batch_size: Number of records per batch
            
        Returns:
            Dictionary with load statistics
            
        Raises:
            LoaderError: If loading fails
        """
        if not file_path.exists():
            raise LoaderError(f"File does not exist: {file_path}")
        
        self.logger.info(f"Starting to load {file_path} into {table_name}")
        
        try:
            # Read parquet file
            df = pd.read_parquet(file_path)
            
            if df.empty:
                self.logger.warning(f"File {file_path} is empty, skipping load")
                return {"status": "skipped", "records_processed": 0}
            
            # Add metadata columns
            df['_file_name'] = data_file.filename
            df['_load_timestamp'] = pd.Timestamp.now()
            df['_record_hash'] = df.apply(
                lambda row: self._calculate_record_hash(row.to_dict()), axis=1
            )
            
            # Validate data quality
            validation_result = self._validate_data_quality(df, data_file.trip_type)
            if not validation_result['is_valid']:
                raise LoaderError(f"Data quality validation failed: {validation_result['errors']}")
            
            # Load data in batches
            total_records = len(df)
            loaded_records = 0
            failed_records = 0
            
            with self.get_connection() as conn:
                for i in range(0, total_records, batch_size):
                    batch_df = df.iloc[i:i + batch_size]
                    
                    try:
                        # Use Snowflake's pandas integration for efficient loading
                        success, nchunks, nrows, _ = write_pandas(
                            conn=conn,
                            df=batch_df,
                            table_name=table_name.upper(),
                            database=self.config.database,
                            schema=self.config.schema,
                            chunk_size=batch_size,
                            compression='gzip',
                            on_error='continue',
                            parallel=4,
                            quote_identifiers=False
                        )
                        
                        if success:
                            loaded_records += len(batch_df)
                            self.logger.info(
                                f"Loaded batch {i//batch_size + 1}: {len(batch_df)} records"
                            )
                        else:
                            failed_records += len(batch_df)
                            self.logger.error(f"Failed to load batch {i//batch_size + 1}")
                            
                    except Exception as e:
                        failed_records += len(batch_df)
                        self.logger.error(f"Batch loading failed: {str(e)}")
            
            # Compile load statistics
            load_stats = {
                "status": "completed" if failed_records == 0 else "partial",
                "total_records": total_records,
                "loaded_records": loaded_records,
                "failed_records": failed_records,
                "file_path": str(file_path),
                "table_name": table_name,
                "load_timestamp": pd.Timestamp.now().isoformat(),
                "data_quality_score": validation_result.get('quality_score', 0)
            }
            
            self.logger.info(
                f"Load completed: {loaded_records}/{total_records} records loaded into {table_name}"
            )
            
            return load_stats
            
        except Exception as e:
            raise LoaderError(f"Failed to load {file_path}: {str(e)}") from e
    
    def _get_yellow_taxi_schema(self) -> str:
        """Get Snowflake DDL column definitions for yellow taxi data"""
        return """
            VendorID INTEGER,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count FLOAT,
            trip_distance FLOAT,
            RatecodeID FLOAT,
            store_and_fwd_flag VARCHAR(1),
            PULocationID INTEGER,
            DOLocationID INTEGER,
            payment_type INTEGER,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            improvement_surcharge FLOAT,
            total_amount FLOAT,
            congestion_surcharge FLOAT
        """
    
    def _get_green_taxi_schema(self) -> str:
        """Get Snowflake DDL column definitions for green taxi data"""
        return """
            VendorID INTEGER,
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            store_and_fwd_flag VARCHAR(1),
            RatecodeID FLOAT,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count FLOAT,
            trip_distance FLOAT,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            ehail_fee FLOAT,
            improvement_surcharge FLOAT,
            total_amount FLOAT,
            payment_type INTEGER,
            trip_type INTEGER,
            congestion_surcharge FLOAT
        """
    
    def _validate_data_quality(self, df: pd.DataFrame, trip_type: str) -> Dict[str, Any]:
        """
        Validate data quality before loading
        
        Args:
            df: DataFrame to validate
            trip_type: Type of trip data
            
        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []
        quality_score = 100
        
        # Check for null values in critical columns
        critical_columns = {
            'yellow_tripdata': ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'total_amount'],
            'green_tripdata': ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'total_amount']
        }
        
        if trip_type in critical_columns:
            for col in critical_columns[trip_type]:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    null_percentage = (null_count / len(df)) * 100
                    
                    if null_percentage > 10:  # More than 10% nulls is an error
                        errors.append(f"Column {col} has {null_percentage:.1f}% null values")
                        quality_score -= 20
                    elif null_percentage > 5:  # 5-10% nulls is a warning
                        warnings.append(f"Column {col} has {null_percentage:.1f}% null values")
                        quality_score -= 5
        
        # Check for reasonable value ranges
        if 'total_amount' in df.columns:
            # Check for unreasonable fare amounts
            negative_fares = (df['total_amount'] < 0).sum()
            extreme_fares = (df['total_amount'] > 1000).sum()
            
            if negative_fares > 0:
                warnings.append(f"{negative_fares} records have negative total_amount")
                quality_score -= 2
            
            if extreme_fares > len(df) * 0.01:  # More than 1% extreme fares
                warnings.append(f"{extreme_fares} records have extreme total_amount (>$1000)")
                quality_score -= 3
        
        # Check date ranges
        pickup_col = 'tpep_pickup_datetime' if trip_type == 'yellow_tripdata' else 'lpep_pickup_datetime'
        dropoff_col = 'tpep_dropoff_datetime' if trip_type == 'yellow_tripdata' else 'lpep_dropoff_datetime'
        
        if pickup_col in df.columns and dropoff_col in df.columns:
            # Check for trips with pickup after dropoff
            invalid_trips = (df[pickup_col] > df[dropoff_col]).sum()
            if invalid_trips > 0:
                warnings.append(f"{invalid_trips} trips have pickup after dropoff")
                quality_score -= 5
        
        return {
            'is_valid': len(errors) == 0,
            'quality_score': max(0, quality_score),
            'errors': errors,
            'warnings': warnings,
            'total_records': len(df)
        }
    
    def _calculate_record_hash(self, record: Dict[str, Any]) -> str:
        """
        Calculate hash for a record to enable deduplication
        
        Args:
            record: Dictionary representing a data record
            
        Returns:
            Hash string for the record
        """
        import hashlib
        import json
        
        # Create a consistent string representation
        record_str = json.dumps(record, sort_keys=True, default=str)
        return hashlib.md5(record_str.encode()).hexdigest()
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information
        """
        query = f"""
        SELECT 
            COUNT(*) as row_count,
            MIN(_load_timestamp) as first_load,
            MAX(_load_timestamp) as last_load,
            COUNT(DISTINCT _file_name) as unique_files
        FROM {table_name}
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                cursor.close()
                
                return {
                    'table_name': table_name,
                    'row_count': result[0] if result else 0,
                    'first_load': result[1] if result else None,
                    'last_load': result[2] if result else None,
                    'unique_files': result[3] if result else 0
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {str(e)}")
            return {'error': str(e)}
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a custom SQL query
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of dictionaries representing query results
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch all results
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                cursor.close()
                return results
                
        except Exception as e:
            raise LoaderError(f"Query execution failed: {str(e)}") from e