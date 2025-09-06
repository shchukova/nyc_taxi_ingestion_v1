"""
Configuration management for NYC Taxi Data Ingestion Pipeline
"""

import os
from dataclasses import dataclass
from typing import Optional, List
from pathlib import Path


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration"""
    account: str
    username: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> 'SnowflakeConfig':
        """Load Snowflake config from environment variables"""
        return cls(
            account=os.getenv('SNOWFLAKE_ACCOUNT', ''),
            username=os.getenv('SNOWFLAKE_USERNAME', ''),
            password=os.getenv('SNOWFLAKE_PASSWORD', ''),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'NYC_TAXI_DB'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'RAW'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )


@dataclass
class S3Config:
    """AWS S3 configuration for external staging"""
    bucket_name: str
    region: str
    access_key_id: str
    secret_access_key: str
    prefix: str = "taxi-data"
    
    @classmethod
    def from_env(cls) -> 'S3Config':
        """Load S3 config from environment variables"""
        return cls(
            bucket_name=os.getenv('S3_BUCKET_NAME', ''),
            region=os.getenv('S3_REGION', 'us-east-1'),
            access_key_id=os.getenv('AWS_ACCESS_KEY_ID', ''),
            secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            prefix=os.getenv('S3_PREFIX', 'taxi-data')
        )


@dataclass
class TLCConfig:
    """NYC TLC data source configuration"""
    base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    trip_types: List[str] = None
    file_format: str = "parquet"
    max_retries: int = 3
    timeout_seconds: int = 300
    
    def __post_init__(self):
        if self.trip_types is None:
            self.trip_types = ["yellow_tripdata", "green_tripdata"]


@dataclass
class PipelineConfig:
    """Main pipeline configuration"""
    data_dir: Path
    batch_size: int = 10000
    max_workers: int = 4
    enable_data_validation: bool = True
    cleanup_temp_files: bool = True
    log_level: str = "INFO"
    
    def __post_init__(self):
        self.data_dir = Path(self.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)


class Settings:
    """
    Main settings class that aggregates all configuration
    """
    
    def __init__(self):
        self.snowflake = SnowflakeConfig.from_env()
        self.s3 = S3Config.from_env()
        self.tlc = TLCConfig()
        self.pipeline = PipelineConfig(
            data_dir=os.getenv('DATA_DIR', './data'),
            batch_size=int(os.getenv('BATCH_SIZE', '10000')),
            max_workers=int(os.getenv('MAX_WORKERS', '4')),
            enable_data_validation=os.getenv('ENABLE_VALIDATION', 'true').lower() == 'true',
            cleanup_temp_files=os.getenv('CLEANUP_TEMP_FILES', 'true').lower() == 'true',
            log_level=os.getenv('LOG_LEVEL', 'INFO')
        )
    
    def validate(self) -> bool:
        """
        Validate that all required configuration is present
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        required_snowflake_fields = [
            self.snowflake.account,
            self.snowflake.username,
            self.snowflake.password
        ]
        
        if not all(required_snowflake_fields):
            return False
            
        required_s3_fields = [
            self.s3.bucket_name,
            self.s3.access_key_id,
            self.s3.secret_access_key
        ]
        
        if not all(required_s3_fields):
            return False
            
        return True


# Global settings instance
settings = Settings()