# src/loaders/stage_manager.py
"""
External stage management for Snowflake data loading
"""

import boto3
from pathlib import Path
from typing import Dict, List, Optional, Any
from botocore.exceptions import ClientError, NoCredentialsError
import snowflake.connector

from src.config.settings import SnowflakeConfig, S3Config
from src.utils.logger import get_logger
from src.utils.exceptions import StageError


class StageManager:
    """
    Manages external staging operations for efficient data loading
    
    This class handles:
    - S3 bucket operations for staging data files
    - Snowflake external stage creation and management
    - File upload and organization in cloud storage
    - Integration between S3 and Snowflake for optimal loading
    - Cleanup and maintenance of staged files
    
    Benefits of external staging:
    - Separates storage from compute for better scalability
    - Enables parallel loading from multiple files
    - Provides better error handling and retry capabilities
    - Reduces Snowflake compute costs by optimizing data transfer
    - Enables data archival and backup strategies
    """
    
    def __init__(self, snowflake_config: SnowflakeConfig, s3_config: S3Config):
        """
        Initialize stage manager
        
        Args:
            snowflake_config: Snowflake configuration
            s3_config: S3 configuration for external staging
        """
        self.snowflake_config = snowflake_config
        self.s3_config = s3_config
        self.logger = get_logger(__name__)
        
        # Initialize S3 client
        self._s3_client = None
        self._initialize_s3_client()
    
    def _initialize_s3_client(self) -> None:
        """Initialize AWS S3 client with credentials"""
        try:
            self._s3_client = boto3.client(
                's3',
                region_name=self.s3_config.region,
                aws_access_key_id=self.s3_config.access_key_id,
                aws_secret_access_key=self.s3_config.secret_access_key
            )
            
            # Test connection by listing bucket (don't fail if bucket doesn't exist yet)
            try:
                self._s3_client.head_bucket(Bucket=self.s3_config.bucket_name)
                self.logger.info(f"Successfully connected to S3 bucket: {self.s3_config.bucket_name}")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    self.logger.warning(f"S3 bucket does not exist: {self.s3_config.bucket_name}")
                else:
                    self.logger.warning(f"Cannot access S3 bucket: {error_code}")
            
        except NoCredentialsError:
            raise StageError("AWS credentials not found or invalid")
        except Exception as e:
            raise StageError(f"Failed to initialize S3 client: {str(e)}") from e
    
    def create_s3_bucket_if_not_exists(self) -> bool:
        """
        Create S3 bucket if it doesn't exist
        
        Returns:
            True if bucket was created or already exists
            
        Raises:
            StageError: If bucket creation fails
        """
        try:
            # Check if bucket already exists
            self._s3_client.head_bucket(Bucket=self.s3_config.bucket_name)
            self.logger.info(f"S3 bucket already exists: {self.s3_config.bucket_name}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    if self.s3_config.region == 'us-east-1':
                        # us-east-1 doesn't need LocationConstraint
                        self._s3_client.create_bucket(Bucket=self.s3_config.bucket_name)
                    else:
                        self._s3_client.create_bucket(
                            Bucket=self.s3_config.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.s3_config.region}
                        )
                    
                    self.logger.info(f"Successfully created S3 bucket: {self.s3_config.bucket_name}")
                    return True
                    
                except ClientError as create_error:
                    raise StageError(f"Failed to create S3 bucket: {str(create_error)}") from create_error
            else:
                raise StageError(f"Error accessing S3 bucket: {error_code}") from e
    
    def upload_file_to_s3(self, local_file_path: Path, s3_key: Optional[str] = None) -> str:
        """
        Upload file to S3 staging area
        
        Args:
            local_file_path: Path to local file to upload
            s3_key: S3 key (path) for the file. If None, uses filename with prefix
            
        Returns:
            S3 key of uploaded file
            
        Raises:
            StageError: If upload fails
        """
        if not local_file_path.exists():
            raise StageError(f"Local file does not exist: {local_file_path}")
        
        if s3_key is None:
            s3_key = f"{self.s3_config.prefix}/{local_file_path.name}"
        
        try:
            # Upload file with progress tracking
            file_size = local_file_path.stat().st_size
            self.logger.info(f"Uploading {local_file_path} to s3://{self.s3_config.bucket_name}/{s3_key}")
            
            self._s3_client.upload_file(
                str(local_file_path),
                self.s3_config.bucket_name,
                s3_key,
                ExtraArgs={
                    'ServerSideEncryption': 'AES256',  # Enable server-side encryption
                    'Metadata': {
                        'original-filename': local_file_path.name,
                        'upload-timestamp': str(pd.Timestamp.now().isoformat()),
                        'file-size-bytes': str(file_size)
                    }
                }
            )
            
            self.logger.info(f"Successfully uploaded file to S3: {s3_key}")
            return s3_key
            
        except ClientError as e:
            raise StageError(f"Failed to upload file to S3: {str(e)}") from e
    
    def create_snowflake_external_stage(self, stage_name: str) -> bool:
        """
        Create external stage in Snowflake pointing to S3 bucket
        
        Args:
            stage_name: Name of the Snowflake stage to create
            
        Returns:
            True if stage was created successfully
            
        Raises:
            StageError: If stage creation fails
        """
        # Construct S3 URL
        s3_url = f"s3://{self.s3_config.bucket_name}/{self.s3_config.prefix}/"
        
        # SQL to create external stage
        create_stage_sql = f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL = '{s3_url}'
        CREDENTIALS = (
            AWS_KEY_ID = '{self.s3_config.access_key_id}'
            AWS_SECRET_KEY = '{self.s3_config.secret_access_key}'
        )
        DIRECTORY = (ENABLE = TRUE)
        FILE_FORMAT = (
            TYPE = 'PARQUET'
            COMPRESSION = 'AUTO'
        )
        """
        
        try:
            with self._get_snowflake_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_stage_sql)
                cursor.close()
            
            self.logger.info(f"Successfully created external stage: {stage_name}")
            return True
            
        except snowflake.connector.errors.Error as e:
            raise StageError(f"Failed to create Snowflake stage: {str(e)}") from e
    
    def list_staged_files(self, stage_name: str) -> List[Dict[str, Any]]:
        """
        List files in Snowflake external stage
        
        Args:
            stage_name: Name of the Snowflake stage
            
        Returns:
            List of dictionaries with file information
        """
        list_files_sql = f"LIST @{stage_name}"
        
        try:
            with self._get_snowflake_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(list_files_sql)
                
                files = []
                for row in cursor.fetchall():
                    files.append({
                        'name': row[0],
                        'size': row[1],
                        'md5': row[2],
                        'last_modified': row[3]
                    })
                
                cursor.close()
                return files
                
        except snowflake.connector.errors.Error as e:
            raise StageError(f"Failed to list staged files: {str(e)}") from e
    
    def copy_from_stage_to_table(
        self, 
        stage_name: str, 
        table_name: str, 
        file_pattern: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Copy data from external stage to Snowflake table
        
        Args:
            stage_name: Name of the external stage
            table_name: Target table name
            file_pattern: Optional file pattern to match (e.g., '*.parquet')
            
        Returns:
            Dictionary with copy statistics
            
        Raises:
            StageError: If copy operation fails
        """
        # Construct file path
        file_path = f"@{stage_name}"
        if file_pattern:
            file_path += f"/{file_pattern}"
        
        copy_sql = f"""
        COPY INTO {table_name}
        FROM {file_path}
        FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'AUTO')
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
        """
        
        try:
            with self._get_snowflake_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(copy_sql)
                
                # Get copy statistics
                results = cursor.fetchall()
                cursor.close()
                
                # Parse results - Snowflake COPY returns statistics
                stats = {
                    'files_loaded': 0,
                    'rows_loaded': 0,
                    'errors_seen': 0,
                    'first_error': None,
                    'files_with_errors': []
                }
                
                for row in results:
                    if len(row) >= 3:  # Standard COPY INTO result format
                        stats['files_loaded'] += 1
                        stats['rows_loaded'] += row[1] if row[1] else 0
                        if row[2]:  # Error count
                            stats['errors_seen'] += row[2]
                            if row[3] and not stats['first_error']:  # First error message
                                stats['first_error'] = row[3]
                            stats['files_with_errors'].append(row[0])
                
                self.logger.info(
                    f"Copy operation completed: {stats['files_loaded']} files, "
                    f"{stats['rows_loaded']} rows loaded, {stats['errors_seen']} errors"
                )
                
                return stats
                
        except snowflake.connector.errors.Error as e:
            raise StageError(f"Failed to copy from stage: {str(e)}") from e
    
    def cleanup_s3_files(self, older_than_days: int = 30) -> int:
        """
        Clean up old files from S3 staging area
        
        Args:
            older_than_days: Delete files older than this many days
            
        Returns:
            Number of files deleted
        """
        import pandas as pd
        
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=older_than_days)
        deleted_count = 0
        
        try:
            # List objects in the bucket with our prefix
            response = self._s3_client.list_objects_v2(
                Bucket=self.s3_config.bucket_name,
                Prefix=self.s3_config.prefix
            )
            
            if 'Contents' not in response:
                self.logger.info("No files found in S3 staging area")
                return 0
            
            # Identify files to delete
            files_to_delete = []
            for obj in response['Contents']:
                if obj['LastModified'].replace(tzinfo=None) < cutoff_date.to_pydatetime():
                    files_to_delete.append({'Key': obj['Key']})
            
            # Delete files in batches (S3 allows max 1000 per batch)
            if files_to_delete:
                for i in range(0, len(files_to_delete), 1000):
                    batch = files_to_delete[i:i + 1000]
                    
                    self._s3_client.delete_objects(
                        Bucket=self.s3_config.bucket_name,
                        Delete={'Objects': batch, 'Quiet': True}
                    )
                    
                    deleted_count += len(batch)
                
                self.logger.info(f"Cleaned up {deleted_count} old files from S3")
            else:
                self.logger.info("No old files found to clean up")
            
            return deleted_count
            
        except ClientError as e:
            self.logger.error(f"Failed to clean up S3 files: {str(e)}")
            return 0
    
    def get_stage_usage_stats(self, stage_name: str) -> Dict[str, Any]:
        """
        Get usage statistics for a Snowflake stage
        
        Args:
            stage_name: Name of the stage
            
        Returns:
            Dictionary with stage statistics
        """
        try:
            staged_files = self.list_staged_files(stage_name)
            
            if not staged_files:
                return {
                    'file_count': 0,
                    'total_size_bytes': 0,
                    'total_size_mb': 0,
                    'oldest_file': None,
                    'newest_file': None
                }
            
            total_size = sum(f['size'] for f in staged_files)
            file_dates = [f['last_modified'] for f in staged_files if f['last_modified']]
            
            stats = {
                'file_count': len(staged_files),
                'total_size_bytes': total_size,
                'total_size_mb': total_size / (1024 * 1024),
                'oldest_file': min(file_dates) if file_dates else None,
                'newest_file': max(file_dates) if file_dates else None,
                'files': staged_files
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get stage statistics: {str(e)}")
            return {'error': str(e)}
    
    def verify_stage_connectivity(self, stage_name: str) -> Dict[str, bool]:
        """
        Verify connectivity between Snowflake stage and S3
        
        Args:
            stage_name: Name of the stage to verify
            
        Returns:
            Dictionary with connectivity test results
        """
        results = {
            's3_bucket_accessible': False,
            'snowflake_stage_exists': False,
            'stage_can_list_files': False
        }
        
        # Test S3 bucket access
        try:
            self._s3_client.head_bucket(Bucket=self.s3_config.bucket_name)
            results['s3_bucket_accessible'] = True
        except Exception as e:
            self.logger.error(f"Cannot access S3 bucket: {str(e)}")
        
        # Test Snowflake stage
        try:
            with self._get_snowflake_connection() as conn:
                cursor = conn.cursor()
                
                # Check if stage exists
                cursor.execute(f"DESCRIBE STAGE {stage_name}")
                results['snowflake_stage_exists'] = True
                
                # Test listing files
                cursor.execute(f"LIST @{stage_name} LIMIT 1")
                results['stage_can_list_files'] = True
                
                cursor.close()
                
        except Exception as e:
            self.logger.error(f"Snowflake stage connectivity test failed: {str(e)}")
        
        return results
    
    def _get_snowflake_connection(self):
        """Get Snowflake connection using configuration"""
        return snowflake.connector.connect(
            account=self.snowflake_config.account,
            user=self.snowflake_config.username,
            password=self.snowflake_config.password,
            warehouse=self.snowflake_config.warehouse,
            database=self.snowflake_config.database,
            schema=self.snowflake_config.schema,
            role=self.snowflake_config.role
        )
    
    def upload_and_stage_file(self, local_file_path: Path, stage_name: str) -> Dict[str, Any]:
        """
        Complete workflow: upload file to S3 and prepare for Snowflake loading
        
        Args:
            local_file_path: Path to local file
            stage_name: Snowflake stage name
            
        Returns:
            Dictionary with operation results
        """
        try:
            # Ensure S3 bucket exists
            self.create_s3_bucket_if_not_exists()
            
            # Upload file to S3
            s3_key = self.upload_file_to_s3(local_file_path)
            
            # Ensure Snowflake stage exists
            self.create_snowflake_external_stage(stage_name)
            
            # Verify the file appears in the stage
            staged_files = self.list_staged_files(stage_name)
            file_found = any(local_file_path.name in f['name'] for f in staged_files)
            
            return {
                'status': 'success',
                'local_file': str(local_file_path),
                's3_key': s3_key,
                'stage_name': stage_name,
                'file_staged': file_found,
                'file_size_bytes': local_file_path.stat().st_size,
                'upload_timestamp': pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'local_file': str(local_file_path)
            }
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        # Any cleanup if needed
        pass