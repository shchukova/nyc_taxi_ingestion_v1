# src/data_sources/tlc_data_source.py
"""
NYC TLC (Taxi & Limousine Commission) Data Source Handler
"""

from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urljoin
import calendar

from src.config.settings import TLCConfig
from src.utils.exceptions import DataSourceError


@dataclass
class TLCDataFile:
    """Represents a TLC data file with metadata"""
    trip_type: str
    year: int
    month: int
    url: str
    filename: str
    estimated_size_mb: Optional[int] = None
    
    @property
    def month_name(self) -> str:
        """Get the month name"""
        return calendar.month_name[self.month]
    
    @property
    def date_string(self) -> str:
        """Get formatted date string (YYYY-MM)"""
        return f"{self.year}-{self.month:02d}"


class TLCDataSource:
    """
    Handles NYC TLC data source operations
    
    This class encapsulates all logic related to the NYC TLC data source:
    - URL generation for different trip types and time periods
    - Data availability checking
    - File metadata management
    - Data validation rules specific to TLC data
    
    The NYC TLC publishes monthly trip data for different vehicle types:
    - Yellow taxi trips
    - Green taxi trips  
    - For-hire vehicle (FHV) trips
    - High volume for-hire vehicle (HVFHV) trips
    """
    
    def __init__(self, config: TLCConfig):
        """
        Initialize TLC data source
        
        Args:
            config: TLC configuration object
        """
        self.config = config
        self._known_file_sizes = self._initialize_file_size_estimates()
    
    def _initialize_file_size_estimates(self) -> Dict[str, int]:
        """
        Initialize estimated file sizes for different trip types
        
        These estimates help with planning storage and processing resources
        
        Returns:
            Dict mapping trip_type to estimated size in MB
        """
        return {
            "yellow_tripdata": 150,  # ~150MB per month on average
            "green_tripdata": 30,   # ~30MB per month on average
            "fhv_tripdata": 200,    # ~200MB per month on average
            "fhvhv_tripdata": 800   # ~800MB per month on average
        }
    
    def generate_file_url(self, trip_type: str, year: int, month: int) -> str:
        """
        Generate URL for a specific TLC data file
        
        Args:
            trip_type: Type of trip data (e.g., 'yellow_tripdata')
            year: Year of the data
            month: Month of the data (1-12)
            
        Returns:
            Complete URL to the data file
            
        Raises:
            DataSourceError: If trip_type is not supported or date is invalid
        """
        if trip_type not in self.config.trip_types:
            raise DataSourceError(f"Unsupported trip type: {trip_type}")
        
        if not self._is_valid_date(year, month):
            raise DataSourceError(f"Invalid date: {year}-{month:02d}")
        
        filename = f"{trip_type}_{year}-{month:02d}.{self.config.file_format}"
        return urljoin(self.config.base_url + "/", filename)
    
    def get_available_files(
        self, 
        trip_type: str, 
        start_date: Tuple[int, int], 
        end_date: Tuple[int, int]
    ) -> List[TLCDataFile]:
        """
        Get list of available TLC data files for a date range
        
        Args:
            trip_type: Type of trip data
            start_date: (year, month) tuple for start date
            end_date: (year, month) tuple for end date
            
        Returns:
            List of TLCDataFile objects
            
        Raises:
            DataSourceError: If parameters are invalid
        """
        if trip_type not in self.config.trip_types:
            raise DataSourceError(f"Unsupported trip type: {trip_type}")
        
        start_year, start_month = start_date
        end_year, end_month = end_date
        
        if not (self._is_valid_date(start_year, start_month) and 
                self._is_valid_date(end_year, end_month)):
            raise DataSourceError("Invalid date range")
        
        if (start_year, start_month) > (end_year, end_month):
            raise DataSourceError("Start date cannot be after end date")
        
        files = []
        current_year, current_month = start_year, start_month
        
        while (current_year, current_month) <= (end_year, end_month):
            url = self.generate_file_url(trip_type, current_year, current_month)
            filename = f"{trip_type}_{current_year}-{current_month:02d}.{self.config.file_format}"
            
            file_info = TLCDataFile(
                trip_type=trip_type,
                year=current_year,
                month=current_month,
                url=url,
                filename=filename,
                estimated_size_mb=self._known_file_sizes.get(trip_type)
            )
            files.append(file_info)
            
            # Move to next month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
        
        return files
    
    def get_recent_files(
        self, 
        trip_type: str, 
        months_back: int = 3
    ) -> List[TLCDataFile]:
        """
        Get recent TLC data files
        
        Args:
            trip_type: Type of trip data
            months_back: Number of months back from current month
            
        Returns:
            List of recent TLCDataFile objects
        """
        today = date.today()
        
        # TLC data is typically published with a 2-month delay
        # So we start from 2 months ago
        end_year = today.year
        end_month = today.month - 2
        
        if end_month <= 0:
            end_month += 12
            end_year -= 1
        
        # Calculate start date
        start_month = end_month - months_back + 1
        start_year = end_year
        
        if start_month <= 0:
            start_month += 12
            start_year -= 1
        
        return self.get_available_files(
            trip_type, 
            (start_year, start_month), 
            (end_year, end_month)
        )
    
    def validate_data_schema(self, trip_type: str) -> Dict[str, str]:
        """
        Get expected schema for a trip type
        
        This method returns the expected column names and data types
        for validation purposes
        
        Args:
            trip_type: Type of trip data
            
        Returns:
            Dictionary mapping column names to expected data types
        """
        if trip_type == "yellow_tripdata":
            return {
                'VendorID': 'int64',
                'tpep_pickup_datetime': 'datetime64[ns]',
                'tpep_dropoff_datetime': 'datetime64[ns]',
                'passenger_count': 'float64',
                'trip_distance': 'float64',
                'RatecodeID': 'float64',
                'store_and_fwd_flag': 'object',
                'PULocationID': 'int64',
                'DOLocationID': 'int64',
                'payment_type': 'int64',
                'fare_amount': 'float64',
                'extra': 'float64',
                'mta_tax': 'float64',
                'tip_amount': 'float64',
                'tolls_amount': 'float64',
                'improvement_surcharge': 'float64',
                'total_amount': 'float64',
                'congestion_surcharge': 'float64'
            }
        elif trip_type == "green_tripdata":
            return {
                'VendorID': 'int64',
                'lpep_pickup_datetime': 'datetime64[ns]',
                'lpep_dropoff_datetime': 'datetime64[ns]',
                'store_and_fwd_flag': 'object',
                'RatecodeID': 'float64',
                'PULocationID': 'int64',
                'DOLocationID': 'int64',
                'passenger_count': 'float64',
                'trip_distance': 'float64',
                'fare_amount': 'float64',
                'extra': 'float64',
                'mta_tax': 'float64',
                'tip_amount': 'float64',
                'tolls_amount': 'float64',
                'ehail_fee': 'float64',
                'improvement_surcharge': 'float64',
                'total_amount': 'float64',
                'payment_type': 'int64',
                'trip_type': 'int64',
                'congestion_surcharge': 'float64'
            }
        else:
            raise DataSourceError(f"Schema not defined for trip type: {trip_type}")
    
    def _is_valid_date(self, year: int, month: int) -> bool:
        """
        Validate if year and month are valid
        
        Args:
            year: Year value
            month: Month value (1-12)
            
        Returns:
            True if valid, False otherwise
        """
        if not (1 <= month <= 12):
            return False
        
        # TLC data starts from around 2009
        if year < 2009 or year > datetime.now().year:
            return False
        
        # Check if the date is not in the future
        # considering TLC publishes with ~2 month delay
        current_date = datetime.now()
        max_available_month = current_date.month - 2
        max_available_year = current_date.year
        
        if max_available_month <= 0:
            max_available_month += 12
            max_available_year -= 1
        
        if (year, month) > (max_available_year, max_available_month):
            return False
        
        return True
    
    def estimate_processing_time(self, files: List[TLCDataFile]) -> int:
        """
        Estimate processing time for a list of files
        
        Args:
            files: List of TLC data files
            
        Returns:
            Estimated processing time in minutes
        """
        total_size_mb = sum(
            file.estimated_size_mb or self._known_file_sizes.get(file.trip_type, 100)
            for file in files
        )
        
        # Rough estimate: 2 minutes per 100MB (download + processing)
        estimated_minutes = (total_size_mb / 100) * 2
        return max(1, int(estimated_minutes))  # At least 1 minute