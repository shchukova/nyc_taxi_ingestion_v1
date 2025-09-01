# scripts/run_ingestion.py
"""
Main execution script for NYC Taxi Data Ingestion Pipeline

This script provides a command-line interface for running the ingestion pipeline
with various options and configurations.

Usage Examples:
    # Ingest recent 3 months of yellow taxi data
    python scripts/run_ingestion.py --trip-type yellow_tripdata --months-back 3

    # Ingest specific date range
    python scripts/run_ingestion.py --trip-type green_tripdata --start-date 2024-01 --end-date 2024-03

    # Run with debug logging
    python scripts/run_ingestion.py --trip-type yellow_tripdata --log-level DEBUG

    # Use direct loading (skip external staging)
    python scripts/run_ingestion.py --trip-type yellow_tripdata --no-staging
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
import json

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestrator.ingestion_pipeline import IngestionPipeline
from src.utils.logger import setup_pipeline_logging, get_logger
from src.utils.exceptions import PipelineError, ConfigurationError


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='NYC Taxi Data Ingestion Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Trip type selection
    parser.add_argument(
        '--trip-type',
        choices=['yellow_tripdata', 'green_tripdata'],
        default='yellow_tripdata',
        help='Type of taxi trip data to ingest (default: yellow_tripdata)'
    )
    
    # Date range options
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        '--months-back',
        type=int,
        default=3,
        help='Number of months back from current date to ingest (default: 3)'
    )
    date_group.add_argument(
        '--date-range',
        nargs=2,
        metavar=('START_DATE', 'END_DATE'),
        help='Specific date range to ingest (format: YYYY-MM YYYY-MM)'
    )
    
    # Processing options
    parser.add_argument(
        '--no-staging',
        action='store_true',
        help='Skip external staging and load directly to Snowflake'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Maximum number of parallel workers (default: 4)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Batch size for data loading (default: 10000)'
    )
    
    # Logging options
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-dir',
        type=str,
        help='Directory for log files (default: console only)'
    )
    
    # Utility operations
    parser.add_argument(
        '--status',
        action='store_true',
        help='Check pipeline status and exit'
    )
    
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Clean up temporary resources and exit'
    )
    
    parser.add_argument(
        '--cleanup-days',
        type=int,
        default=7,
        help='Clean up files older than this many days (default: 7)'
    )
    
    # Configuration validation
    parser.add_argument(
        '--validate-config',
        action='store_true',
        help='Validate configuration and exit'
    )
    
    # Output options
    parser.add_argument(
        '--output-format',
        choices=['text', 'json'],
        default='text',
        help='Output format (default: text)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be processed without actually running'
    )
    
    return parser.parse_args()


def validate_date_range(start_date: str, end_date: str) -> tuple[int, int, int, int]:
    """
    Validate and parse date range arguments
    
    Args:
        start_date: Start date in YYYY-MM format
        end_date: End date in YYYY-MM format
        
    Returns:
        Tuple of (start_year, start_month, end_year, end_month)
    """
    try:
        start_parts = start_date.split('-')
        end_parts = end_date.split('-')
        
        if len(start_parts) != 2 or len(end_parts) != 2:
            raise ValueError("Date format must be YYYY-MM")
        
        start_year, start_month = int(start_parts[0]), int(start_parts[1])
        end_year, end_month = int(end_parts[0]), int(end_parts[1])
        
        # Validate month values
        if not (1 <= start_month <= 12) or not (1 <= end_month <= 12):
            raise ValueError("Month must be between 1 and 12")
        
        # Validate date order
        if (start_year, start_month) > (end_year, end_month):
            raise ValueError("Start date must be before or equal to end date")
        
        return start_year, start_month, end_year, end_month
        
    except ValueError as e:
        raise ConfigurationError(f"Invalid date range: {e}")


def setup_environment(args):
    """Setup environment based on command line arguments"""
    # Setup logging
    setup_pipeline_logging(log_level=args.log_level, log_dir=args.log_dir)
    
    # Override configuration with command line arguments
    from src.config.settings import settings
    
    if args.max_workers:
        settings.pipeline.max_workers = args.max_workers
    
    if args.batch_size:
        settings.pipeline.batch_size = args.batch_size
    
    settings.pipeline.log_level = args.log_level


def print_status(status: dict, output_format: str):
    """Print pipeline status"""
    if output_format == 'json':
        print(json.dumps(status, indent=2, default=str))
    else:
        print("=== Pipeline Status ===")
        print(f"Status: {status['pipeline_status']}")
        print(f"Configuration Valid: {status['configuration_valid']}")
        print(f"Snowflake Connectivity: {status.get('snowflake_connectivity', 'Unknown')}")
        print(f"S3 Connectivity: {status.get('s3_connectivity', 'Unknown')}")
        print(f"Data Directory: {status['data_directory']}")
        print(f"Log Level: {status['log_level']}")
        print(f"Max Workers: {status['max_workers']}")
        print(f"Timestamp: {status['timestamp']}")
        
        if status['pipeline_status'] == 'unhealthy':
            print(f"Error: {status.get('error', 'Unknown error')}")


def print_results(result, output_format: str):
    """Print ingestion results"""
    if output_format == 'json':
        # Convert result to dict for JSON serialization
        result_dict = {
            'status': result.status,
            'files_processed': result.files_processed,
            'total_records': result.total_records,
            'errors': result.errors,
            'warnings': result.warnings,
            'processing_time_seconds': result.processing_time_seconds,
            'data_quality_metrics': result.data_quality_metrics
        }
        print(json.dumps(result_dict, indent=2, default=str))
    else:
        print("=== Ingestion Results ===")
        print(f"Status: {result.status}")
        print(f"Files Processed: {result.files_processed}")
        print(f"Total Records: {result.total_records:,}")
        print(f"Processing Time: {result.processing_time_seconds:.2f} seconds")
        
        if result.total_records > 0:
            records_per_second = result.total_records / result.processing_time_seconds
            print(f"Records per Second: {records_per_second:,.0f}")
        
        if result.errors:
            print(f"Errors: {len(result.errors)}")
            for error in result.errors[:3]:  # Show first 3 errors
                print(f"  - {error.get('message', 'Unknown error')}")
            if len(result.errors) > 3:
                print(f"  ... and {len(result.errors) - 3} more errors")
        
        if result.warnings:
            print(f"Warnings: {len(result.warnings)}")
            for warning in result.warnings[:3]:  # Show first 3 warnings
                print(f"  - {warning.get('message', 'Unknown warning')}")
            if len(result.warnings) > 3:
                print(f"  ... and {len(result.warnings) - 3} more warnings")
        
        # Data quality metrics
        if result.data_quality_metrics:
            print("\n=== Data Quality Metrics ===")
            for key, value in result.data_quality_metrics.items():
                if key != 'quality_check_failed':
                    print(f"{key.replace('_', ' ').title()}: {value}")


def run_dry_run(args):
    """Run in dry-run mode to show what would be processed"""
    logger = get_logger(__name__)
    
    try:
        from src.data_sources.tlc_data_source import TLCDataSource
        from src.config.settings import settings
        
        data_source = TLCDataSource(settings.tlc)
        
        if args.date_range:
            start_year, start_month, end_year, end_month = validate_date_range(*args.date_range)
            files = data_source.get_available_files(
                args.trip_type,
                (start_year, start_month),
                (end_year, end_month)
            )
        else:
            files = data_source.get_recent_files(args.trip_type, args.months_back)
        
        print("=== Dry Run - Files to Process ===")
        print(f"Trip Type: {args.trip_type}")
        print(f"Total Files: {len(files)}")
        
        total_size = 0
        for file_info in files:
            size_mb = file_info.estimated_size_mb or 100
            total_size += size_mb
            print(f"  - {file_info.filename} (~{size_mb}MB)")
        
        print(f"\nEstimated Total Size: {total_size}MB")
        
        estimated_time = data_source.estimate_processing_time(files)
        print(f"Estimated Processing Time: {estimated_time} minutes")
        
        print(f"Use External Staging: {not args.no_staging}")
        print(f"Max Workers: {args.max_workers}")
        print(f"Batch Size: {args.batch_size}")
        
    except Exception as e:
        logger.error(f"Dry run failed: {str(e)}")
        return 1
    
    return 0


def main():
    """Main entry point"""
    args = parse_arguments()
    
    try:
        # Setup environment
        setup_environment(args)
        logger = get_logger(__name__)
        
        logger.info("Starting NYC Taxi Data Ingestion Pipeline")
        logger.info(f"Arguments: {vars(args)}")
        
        # Handle utility operations first
        if args.validate_config:
            from src.config.settings import settings
            if settings.validate():
                print("✓ Configuration is valid")
                return 0
            else:
                print("✗ Configuration is invalid - check required environment variables")
                return 1
        
        if args.dry_run:
            return run_dry_run(args)
        
        # Initialize pipeline
        pipeline = IngestionPipeline()
        
        if args.status:
            status = pipeline.get_pipeline_status()
            print_status(status, args.output_format)
            return 0 if status['pipeline_status'] == 'healthy' else 1
        
        if args.cleanup:
            cleanup_results = pipeline.cleanup_resources(args.cleanup_days)
            if args.output_format == 'json':
                print(json.dumps(cleanup_results, indent=2))
            else:
                print("=== Cleanup Results ===")
                print(f"Status: {cleanup_results['status']}")
                print(f"Temp files cleaned: {cleanup_results.get('temp_files_cleaned', 0)}")
                print(f"S3 files cleaned: {cleanup_results.get('s3_files_cleaned', 0)}")
                if cleanup_results['status'] == 'error':
                    print(f"Error: {cleanup_results.get('error')}")
            return 0 if cleanup_results['status'] == 'success' else 1
        
        # Run main ingestion
        if args.date_range:
            start_year, start_month, end_year, end_month = validate_date_range(*args.date_range)
            result = pipeline.ingest_date_range(
                trip_type=args.trip_type,
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month,
                use_external_stage=not args.no_staging
            )
        else:
            result = pipeline.ingest_recent_data(
                trip_type=args.trip_type,
                months_back=args.months_back,
                use_external_stage=not args.no_staging
            )
        
        # Print results
        print_results(result, args.output_format)
        
        # Return appropriate exit code
        if result.status == 'completed':
            logger.info("Pipeline completed successfully")
            return 0
        elif result.status == 'completed_with_errors':
            logger.warning("Pipeline completed with errors")
            return 1
        else:
            logger.error("Pipeline failed")
            return 2
    
    except ConfigurationError as e:
        print(f"Configuration Error: {e}")
        return 1
    
    except PipelineError as e:
        print(f"Pipeline Error: {e}")
        return 2
    
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
        return 130
    
    except Exception as e:
        print(f"Unexpected error: {e}")
        if args.log_level == 'DEBUG':
            import traceback
            traceback.print_exc()
        return 3


if __name__ == '__main__':
    sys.exit(main())