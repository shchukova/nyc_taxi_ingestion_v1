# src/utils/logger.py
"""
Centralized logging configuration for NYC Taxi Data Pipeline
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime
import json


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging
    
    Provides structured logging output that's easily parseable
    by log aggregation systems like ELK stack, Splunk, or CloudWatch
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log string
        """
        log_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'process_id': record.process,
            'thread_id': record.thread
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields from LoggerAdapter or custom fields
        if hasattr(record, '__dict__'):
            for key, value in record.__dict__.items():
                if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 
                              'pathname', 'filename', 'module', 'lineno', 
                              'funcName', 'created', 'msecs', 'relativeCreated',
                              'thread', 'threadName', 'processName', 'process',
                              'getMessage', 'exc_info', 'exc_text', 'stack_info']:
                    log_entry[key] = value
        
        return json.dumps(log_entry)


class PipelineLogger:
    """
    Pipeline-specific logger configuration
    
    Provides:
    - Consistent logging format across all pipeline components
    - File and console logging
    - Log rotation for production environments
    - Performance and audit logging capabilities
    - Integration with monitoring systems
    """
    
    def __init__(self, log_level: str = "INFO", log_dir: Optional[Path] = None):
        """
        Initialize pipeline logger
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_dir: Directory for log files (optional)
        """
        self.log_level = log_level.upper()
        self.log_dir = log_dir
        self._configure_logging()
    
    def _configure_logging(self) -> None:
        """Configure logging handlers and formatters"""
        # Create root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.log_level))
        
        # Clear existing handlers to avoid duplication
        root_logger.handlers.clear()
        
        # Console handler with colored output
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, self.log_level))
        
        # Use JSON formatter for production, simple formatter for development
        if self.log_level == "DEBUG":
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        else:
            console_formatter = JSONFormatter()
        
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # File handler if log directory is specified
        if self.log_dir:
            self.log_dir = Path(self.log_dir)
            self.log_dir.mkdir(parents=True, exist_ok=True)
            
            # Main application log
            file_handler = logging.handlers.RotatingFileHandler(
                filename=self.log_dir / 'nyc_taxi_pipeline.log',
                maxBytes=50 * 1024 * 1024,  # 50MB
                backupCount=10,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(JSONFormatter())
            root_logger.addHandler(file_handler)
            
            # Error log
            error_handler = logging.handlers.RotatingFileHandler(
                filename=self.log_dir / 'errors.log',
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(JSONFormatter())
            root_logger.addHandler(error_handler)
        
        # Suppress noisy third-party loggers
        logging.getLogger('boto3').setLevel(logging.WARNING)
        logging.getLogger('botocore').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('snowflake').setLevel(logging.INFO)


class PerformanceLogger:
    """
    Performance and metrics logger for pipeline monitoring
    
    Tracks:
    - Processing times for different pipeline stages
    - Data volume metrics
    - Error rates and types
    - Resource utilization
    """
    
    def __init__(self, logger_name: str):
        """
        Initialize performance logger
        
        Args:
            logger_name: Name for the logger instance
        """
        self.logger = logging.getLogger(f"performance.{logger_name}")
        self.start_times = {}
    
    def start_operation(self, operation_name: str) -> None:
        """
        Start timing an operation
        
        Args:
            operation_name: Name of the operation to time
        """
        self.start_times[operation_name] = datetime.utcnow()
        self.logger.info(f"Started operation: {operation_name}")
    
    def end_operation(self, operation_name: str, **extra_metrics) -> float:
        """
        End timing an operation and log metrics
        
        Args:
            operation_name: Name of the operation
            **extra_metrics: Additional metrics to log
            
        Returns:
            Duration in seconds
        """
        if operation_name not in self.start_times:
            self.logger.warning(f"Operation {operation_name} was not started")
            return 0.0
        
        end_time = datetime.utcnow()
        duration = (end_time - self.start_times[operation_name]).total_seconds()
        
        metrics = {
            'operation': operation_name,
            'duration_seconds': duration,
            'start_time': self.start_times[operation_name].isoformat(),
            'end_time': end_time.isoformat(),
            **extra_metrics
        }
        
        self.logger.info(f"Completed operation: {operation_name}", extra=metrics)
        del self.start_times[operation_name]
        
        return duration
    
    def log_data_metrics(self, **metrics) -> None:
        """
        Log data processing metrics
        
        Args:
            **metrics: Data metrics to log
        """
        self.logger.info("Data metrics", extra={'metrics_type': 'data', **metrics})
    
    def log_error_metrics(self, error_type: str, error_message: str, **context) -> None:
        """
        Log error metrics for monitoring
        
        Args:
            error_type: Type/category of error
            error_message: Error message
            **context: Additional context information
        """
        error_metrics = {
            'metrics_type': 'error',
            'error_type': error_type,
            'error_message': error_message,
            **context
        }
        
        self.logger.error("Error occurred", extra=error_metrics)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a module
    
    Args:
        name: Usually __name__ from the calling module
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


def setup_pipeline_logging(log_level: str = "INFO", log_dir: Optional[str] = None) -> None:
    """
    Setup logging for the entire pipeline
    
    Call this once at the start of your application
    
    Args:
        log_level: Logging level
        log_dir: Directory for log files
    """
    log_dir_path = Path(log_dir) if log_dir else None
    PipelineLogger(log_level=log_level, log_dir=log_dir_path)


# Context manager for operation timing
class timed_operation:
    """
    Context manager for timing operations
    
    Usage:
        with timed_operation("data_download", logger) as timer:
            # Do work here
            pass
        # Duration is automatically logged
    """
    
    def __init__(self, operation_name: str, logger: logging.Logger):
        self.operation_name = operation_name
        self.logger = logger
        self.start_time = None
        self.performance_logger = PerformanceLogger(logger.name)
    
    def __enter__(self):
        self.performance_logger.start_operation(self.operation_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = self.performance_logger.end_operation(
            self.operation_name,
            success=exc_type is None,
            error_type=exc_type.__name__ if exc_type else None
        )