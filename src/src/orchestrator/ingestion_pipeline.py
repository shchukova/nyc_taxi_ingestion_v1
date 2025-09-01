# src/orchestrator/ingestion_pipeline.py
"""
Main orchestrator for NYC Taxi Data Ingestion Pipeline
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import concurrent.futures
from dataclasses import dataclass

from src.config.settings import settings
from src.data_sources.tlc_data_source import TLCDataSource, TLCDataFile
from src.extractors.file_extractor import FileExtractor
from src.loaders.snowflake_loader import SnowflakeLoader
from src.loaders.stage_manager import StageManager
from src.models.taxi_trip import TripType
from src.utils.logger import get_logger, PerformanceLogger, timed_operation
from src.utils.exceptions import PipelineError, ErrorCollector, ConfigurationError


@dataclass
class IngestionResult:
    """Results from an ingestion operation"""
    status: str
    files_processed: int
    total_records: int
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    processing_time_seconds: float
    data_quality_metrics: Dict[str, Any]


class IngestionPipeline:
    """
    Main orchestrator for the NYC Taxi data ingestion pipeline
    
    This class coordinates all pipeline components:
    - Data source management and file discovery
    - File extraction and downloading
    - External staging (S3) and Snowflake loading  
    - Error handling and retry logic
    - Performance monitoring and logging
    - Data quality validation and reporting
    
    Key Features:
    - Parallel processing for multiple files
    - Comprehensive error handling with rollback
    - Progress tracking and monitoring
    - Configurable batch processing
    - Data lineage and audit logging
    - Integration with external monitoring systems
    """
    
    def __init__(self):
        """Initialize the ingestion pipeline"""
        self.logger = get_logger(__name__)
        self.performance_logger = PerformanceLogger(__name__)
        
        # Validate configuration
        if not settings.validate():
            raise ConfigurationError("Invalid configuration - check required environment variables")
        
        # Initialize components
        self.data_source = TLCDataSource(settings.tlc)
        self.file_extractor = FileExtractor(settings.tlc, settings.pipeline.data_dir)
        self.snowflake_loader = SnowflakeLoader(settings.snowflake)
        self.stage_manager = StageManager(settings.snowflake, settings.s3)
        
        # Error collector for batch operations
        self.error_collector = ErrorCollector()
        
        self.logger.info("Ingestion pipeline initialized successfully")
    
    def ingest_recent_data(
        self, 
        trip_type: str = "yellow_tripdata",
        months_back: int = 3,
        use_external_stage: bool = True
    ) -> IngestionResult:
        """
        Ingest recent NYC taxi data
        
        Args:
            trip_type: Type of taxi data to ingest
            months_back: Number of months back from current date
            use_external_stage: Whether to use S3 external staging
            
        Returns:
            IngestionResult with processing statistics
        """
        with timed_operation("ingest_recent_data", self.logger):
            self.logger.info(f"Starting ingestion of recent {trip_type} data ({months_back} months)")
            
            try:
                # Get list of files to process
                available_files = self.data_source.get_recent_files(trip_type, months_back)
                
                if not available_files:
                    self.logger.warning("No files found for ingestion")
                    return IngestionResult(
                        status="completed",
                        files_processed=0,
                        total_records=0,
                        errors=[],
                        warnings=[{"message": "No files found for processing"}],
                        processing_time_seconds=0,
                        data_quality_metrics={}
                    )
                
                # Process files
                return self._process_file_batch(available_files, use_external_stage)
                
            except Exception as e:
                error_msg = f"Failed to ingest recent data: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                raise PipelineError(error_msg, cause=e)
    
    def ingest_date_range(
        self,
        trip_type: str,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        use_external_stage: bool = True
    ) -> IngestionResult:
        """
        Ingest taxi data for a specific date range
        
        Args:
            trip_type: Type of taxi data
            start_year: Start year
            start_month: Start month (1-12)
            end_year: End year
            end_month: End month (1-12)
            use_external_stage: Whether to use S3 external staging
            
        Returns:
            IngestionResult with processing statistics
        """
        with timed_operation("ingest_date_range", self.logger):
            self.logger.info(
                f"Starting ingestion of {trip_type} data from "
                f"{start_year}-{start_month:02d} to {end_year}-{end_month:02d}"
            )
            
            try:
                # Get list of files for date range
                available_files = self.data_source.get_available_files(
                    trip_type,
                    (start_year, start_month),
                    (end_year, end_month)
                )
                
                # Process files
                return self._process_file_batch(available_files, use_external_stage)
                
            except Exception as e:
                error_msg = f"Failed to ingest date range: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                raise PipelineError(error_msg, cause=e)
    
    def _process_file_batch(
        self, 
        files: List[TLCDataFile], 
        use_external_stage: bool
    ) -> IngestionResult:
        """
        Process a batch of TLC data files
        
        Args:
            files: List of data files to process
            use_external_stage: Whether to use external staging
            
        Returns:
            IngestionResult with processing statistics
        """
        start_time = datetime.utcnow()
        total_records = 0
        processed_files = 0
        
        self.logger.info(f"Processing batch of {len(files)} files")
        
        # Create table for the trip type
        table_name = f"raw_{files[0].trip_type}"
        self.snowflake_loader.create_raw_table(table_name, files[0].trip_type)
        
        # Process files in parallel or sequentially based on configuration
        if settings.pipeline.max_workers > 1:
            processed_files, total_records = self._process_files_parallel(
                files, table_name, use_external_stage
            )
        else:
            processed_files, total_records = self._process_files_sequential(
                files, table_name, use_external_stage
            )
        
        # Calculate processing time
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        # Generate data quality metrics
        quality_metrics = self._generate_quality_metrics(table_name, processed_files, total_records)
        
        # Determine overall status
        status = "completed"
        if self.error_collector.has_errors:
            status = "completed_with_errors" if processed_files > 0 else "failed"
        
        # Create result
        result = IngestionResult(
            status=status,
            files_processed=processed_files,
            total_records=total_records,
            errors=[error.to_dict() for error in self.error_collector.errors],
            warnings=self.error_collector.warnings,
            processing_time_seconds=processing_time,
            data_quality_metrics=quality_metrics
        )
        
        # Log final results
        self.performance_logger.log_data_metrics(
            files_processed=processed_files,
            total_records=total_records,
            processing_time_seconds=processing_time,
            records_per_second=total_records / processing_time if processing_time > 0 else 0,
            errors=len(result.errors),
            warnings=len(result.warnings)
        )
        
        self.logger.info(f"Batch processing completed: {result.status}")
        return result
    
    def _process_files_sequential(
        self, 
        files: List[TLCDataFile], 
        table_name: str, 
        use_external_stage: bool
    ) -> tuple[int, int]:
        """Process files sequentially"""
        processed_files = 0
        total_records = 0
        
        for file_info in files:
            try:
                records = self._process_single_file(file_info, table_name, use_external_stage)
                total_records += records
                processed_files += 1
                
                self.logger.info(f"Successfully processed {file_info.filename}: {records} records")
                
            except Exception as e:
                self.error_collector.add_error(e, {'filename': file_info.filename})
                self.logger.error(f"Failed to process {file_info.filename}: {str(e)}")
                continue
        
        return processed_files, total_records
    
    def _process_files_parallel(
        self, 
        files: List[TLCDataFile], 
        table_name: str, 
        use_external_stage: bool
    ) -> tuple[int, int]:
        """Process files in parallel"""
        processed_files = 0
        total_records = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=settings.pipeline.max_workers) as executor:
            # Submit all file processing tasks
            future_to_file = {
                executor.submit(self._process_single_file, file_info, table_name, use_external_stage): file_info
                for file_info in files
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                file_info = future_to_file[future]
                
                try:
                    records = future.result()
                    total_records += records
                    processed_files += 1
                    
                    self.logger.info(f"Successfully processed {file_info.filename}: {records} records")
                    
                except Exception as e:
                    self.error_collector.add_error(e, {'filename': file_info.filename})
                    self.logger.error(f"Failed to process {file_info.filename}: {str(e)}")
        
        return processed_files, total_records
    
    def _process_single_file(
        self, 
        file_info: TLCDataFile, 
        table_name: str, 
        use_external_stage: bool
    ) -> int:
        """
        Process a single data file
        
        Args:
            file_info: File information
            table_name: Target table name
            use_external_stage: Whether to use external staging
            
        Returns:
            Number of records processed
        """
        with timed_operation(f"process_file_{file_info.filename}", self.logger):
            
            # Step 1: Download file
            local_file_path = self.file_extractor.download_file(file_info)
            
            try:
                if use_external_stage:
                    # Step 2a: Upload to S3 and use external stage
                    stage_name = f"{table_name}_stage"
                    
                    # Upload to S3 and create stage
                    stage_result = self.stage_manager.upload_and_stage_file(local_file_path, stage_name)
                    
                    if stage_result['status'] != 'success':
                        raise PipelineError(f"Failed to stage file: {stage_result.get('error')}")
                    
                    # Copy from stage to table
                    copy_stats = self.stage_manager.copy_from_stage_to_table(
                        stage_name, 
                        table_name, 
                        file_info.filename
                    )
                    
                    return copy_stats['rows_loaded']
                    
                else:
                    # Step 2b: Direct load to Snowflake
                    load_stats = self.snowflake_loader.load_parquet_file(
                        local_file_path,
                        table_name,
                        file_info,
                        settings.pipeline.batch_size
                    )
                    
                    return load_stats['loaded_records']
                    
            finally:
                # Cleanup local file if configured
                if settings.pipeline.cleanup_temp_files and local_file_path.exists():
                    local_file_path.unlink()
                    self.logger.debug(f"Cleaned up temporary file: {local_file_path}")
    
    def _generate_quality_metrics(
        self, 
        table_name: str, 
        processed_files: int, 
        total_records: int
    ) -> Dict[str, Any]:
        """Generate data quality metrics"""
        try:
            table_info = self.snowflake_loader.get_table_info(table_name)
            
            return {
                'files_processed': processed_files,
                'total_records': total_records,
                'table_row_count': table_info.get('row_count', 0),
                'unique_files_in_table': table_info.get('unique_files', 0),
                'data_consistency_check': table_info.get('row_count', 0) == total_records,
                'first_load_time': table_info.get('first_load'),
                'last_load_time': table_info.get('last_load')
            }
        except Exception as e:
            self.logger.warning(f"Failed to generate quality metrics: {str(e)}")
            return {
                'files_processed': processed_files,
                'total_records': total_records,
                'quality_check_failed': True,
                'error': str(e)
            }
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and health"""
        try:
            # Test connectivity
            stage_connectivity = self.stage_manager.verify_stage_connectivity("test_stage")
            
            return {
                'pipeline_status': 'healthy',
                'configuration_valid': settings.validate(),
                'snowflake_connectivity': stage_connectivity.get('snowflake_stage_exists', False),
                's3_connectivity': stage_connectivity.get('s3_bucket_accessible', False),
                'data_directory': str(settings.pipeline.data_dir),
                'log_level': settings.pipeline.log_level,
                'max_workers': settings.pipeline.max_workers,
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                'pipeline_status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    def cleanup_resources(self, older_than_days: int = 7) -> Dict[str, Any]:
        """Clean up temporary resources"""
        cleanup_results = {}
        
        try:
            # Clean up temporary files
            temp_files_cleaned = self.file_extractor.cleanup_temp_files()
            cleanup_results['temp_files_cleaned'] = temp_files_cleaned
            
            # Clean up S3 staged files
            s3_files_cleaned = self.stage_manager.cleanup_s3_files(older_than_days)
            cleanup_results['s3_files_cleaned'] = s3_files_cleaned
            
            cleanup_results['status'] = 'success'
            self.logger.info(f"Cleanup completed: {cleanup_results}")
            
        except Exception as e:
            cleanup_results['status'] = 'error'
            cleanup_results['error'] = str(e)
            self.logger.error(f"Cleanup failed: {str(e)}")
        
        return cleanup_results