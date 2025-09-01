# src/utils/exceptions.py
"""
Custom exceptions for NYC Taxi Data Pipeline
"""

from typing import Optional, Dict, Any


class PipelineError(Exception):
    """
    Base exception for all pipeline errors
    
    Provides structured error handling with context information
    for better debugging and monitoring
    """
    
    def __init__(
        self, 
        message: str, 
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None
    ):
        """
        Initialize pipeline error
        
        Args:
            message: Human-readable error message
            error_code: Machine-readable error code for categorization
            context: Additional context information
            cause: Original exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.context = context or {}
        self.cause = cause
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for logging/serialization"""
        return {
            'error_type': self.__class__.__name__,
            'error_code': self.error_code,
            'message': self.message,
            'context': self.context,
            'cause': str(self.cause) if self.cause else None
        }
    
    def __str__(self) -> str:
        """String representation of the error"""
        base_msg = f"{self.error_code}: {self.message}"
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            base_msg += f" (Context: {context_str})"
        if self.cause:
            base_msg += f" (Caused by: {self.cause})"
        return base_msg


class ConfigurationError(PipelineError):
    """
    Raised when there are configuration issues
    
    Examples:
    - Missing required configuration parameters
    - Invalid configuration values
    - Inconsistent configuration settings
    """
    pass


class DataSourceError(PipelineError):
    """
    Raised when there are data source related issues
    
    Examples:
    - Invalid data source URLs
    - Data source unavailable
    - Unsupported data formats
    - Authentication failures
    """
    pass


class ExtractionError(PipelineError):
    """
    Raised during data extraction/download operations
    
    Examples:
    - Network connectivity issues
    - File download failures
    - File corruption during transfer
    - Insufficient storage space
    """
    pass


class LoaderError(PipelineError):
    """
    Raised during data loading operations
    
    Examples:
    - Database connection failures
    - SQL execution errors
    - Data type conversion failures
    - Schema validation errors
    """
    pass


class StageError(PipelineError):
    """
    Raised during staging operations
    
    Examples:
    - S3 access issues
    - Snowflake stage creation failures
    - File staging problems
    - Permission errors
    """
    pass


class ValidationError(PipelineError):
    """
    Raised when data validation fails
    
    Examples:
    - Schema validation failures
    - Data quality issues
    - Business rule violations
    - Missing required fields
    """
    pass


class ProcessingError(PipelineError):
    """
    Raised during data processing operations
    
    Examples:
    - Data transformation failures
    - Memory allocation issues
    - Processing timeouts
    - Resource exhaustion
    """
    pass


# Utility functions for exception handling

def handle_pipeline_exception(
    func_name: str, 
    exception: Exception, 
    context: Optional[Dict[str, Any]] = None
) -> PipelineError:
    """
    Convert generic exceptions to pipeline-specific exceptions
    
    Args:
        func_name: Name of the function where error occurred
        exception: Original exception
        context: Additional context information
        
    Returns:
        Appropriate PipelineError subclass
    """
    error_context = {
        'function': func_name,
        **(context or {})
    }
    
    # Map common exception types to pipeline exceptions
    if isinstance(exception, (ConnectionError, TimeoutError)):
        return ExtractionError(
            f"Network error in {func_name}: {str(exception)}",
            error_code="NETWORK_ERROR",
            context=error_context,
            cause=exception
        )
    
    elif isinstance(exception, FileNotFoundError):
        return ExtractionError(
            f"File not found in {func_name}: {str(exception)}",
            error_code="FILE_NOT_FOUND",
            context=error_context,
            cause=exception
        )
    
    elif isinstance(exception, PermissionError):
        return StageError(
            f"Permission denied in {func_name}: {str(exception)}",
            error_code="PERMISSION_DENIED",
            context=error_context,
            cause=exception
        )
    
    elif isinstance(exception, ValueError):
        return ValidationError(
            f"Data validation error in {func_name}: {str(exception)}",
            error_code="VALIDATION_ERROR",
            context=error_context,
            cause=exception
        )
    
    elif isinstance(exception, MemoryError):
        return ProcessingError(
            f"Memory error in {func_name}: {str(exception)}",
            error_code="MEMORY_ERROR",
            context=error_context,
            cause=exception
        )
    
    else:
        # Generic pipeline error for unknown exceptions
        return PipelineError(
            f"Unexpected error in {func_name}: {str(exception)}",
            error_code="UNKNOWN_ERROR",
            context=error_context,
            cause=exception
        )


def retry_on_exception(
    max_retries: int = 3,
    delay_seconds: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator for retrying functions on specific exceptions
    
    Args:
        max_retries: Maximum number of retry attempts
        delay_seconds: Initial delay between retries
        backoff_factor: Multiplier for delay on each retry
        exceptions: Tuple of exception types to retry on
        
    Returns:
        Decorated function with retry logic
    """
    import time
    import functools
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        # Final attempt failed, raise the exception
                        break
                    
                    # Calculate delay with exponential backoff
                    delay = delay_seconds * (backoff_factor ** attempt)
                    time.sleep(delay)
            
            # Convert to pipeline exception and raise
            pipeline_error = handle_pipeline_exception(
                func.__name__,
                last_exception,
                {'max_retries': max_retries, 'final_attempt': True}
            )
            raise pipeline_error
        
        return wrapper
    return decorator


class ErrorCollector:
    """
    Utility class for collecting and managing multiple errors
    
    Useful for batch operations where you want to collect all errors
    rather than failing on the first one
    """
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def add_error(self, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Add an error to the collection"""
        if isinstance(error, PipelineError):
            if context:
                error.context.update(context)
            self.errors.append(error)
        else:
            pipeline_error = handle_pipeline_exception("batch_operation", error, context)
            self.errors.append(pipeline_error)
    
    def add_warning(self, message: str, context: Optional[Dict[str, Any]] = None):
        """Add a warning to the collection"""
        warning = {
            'message': message,
            'context': context or {}
        }
        self.warnings.append(warning)
    
    @property
    def has_errors(self) -> bool:
        """Check if any errors were collected"""
        return len(self.errors) > 0
    
    @property
    def has_warnings(self) -> bool:
        """Check if any warnings were collected"""
        return len(self.warnings) > 0
    
    @property
    def error_count(self) -> int:
        """Get total number of errors"""
        return len(self.errors)
    
    @property
    def warning_count(self) -> int:
        """Get total number of warnings"""
        return len(self.warnings)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of collected errors and warnings"""
        return {
            'error_count': self.error_count,
            'warning_count': self.warning_count,
            'errors': [error.to_dict() for error in self.errors],
            'warnings': self.warnings
        }
    
    def raise_if_errors(self):
        """Raise an exception if any errors were collected"""
        if self.has_errors:
            summary = self.get_summary()
            raise ProcessingError(
                f"Batch operation failed with {self.error_count} errors",
                error_code="BATCH_ERRORS",
                context=summary
            )
    
    def clear(self):
        """Clear all collected errors and warnings"""
        self.errors.clear()
        self.warnings.clear()