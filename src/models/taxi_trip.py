# src/models/taxi_trip.py
"""
Data models for NYC Taxi Trip records
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
import pandas as pd


class TripType(Enum):
    """Enumeration for trip types"""
    YELLOW = "yellow_tripdata"
    GREEN = "green_tripdata"
    FHV = "fhv_tripdata"  # For-hire vehicle
    HVFHV = "fhvhv_tripdata"  # High volume for-hire vehicle


class PaymentType(Enum):
    """Payment type enumeration based on TLC data dictionary"""
    CREDIT_CARD = 1
    CASH = 2
    NO_CHARGE = 3
    DISPUTE = 4
    UNKNOWN = 5
    VOIDED_TRIP = 6


class RateCodeType(Enum):
    """Rate code enumeration"""
    STANDARD_RATE = 1
    JFK = 2
    NEWARK = 3
    NASSAU_WESTCHESTER = 4
    NEGOTIATED_FARE = 5
    GROUP_RIDE = 6


@dataclass
class TaxiTrip:
    """
    Base data model for taxi trip records
    
    This class provides:
    - Type safety for taxi trip data
    - Validation methods for data quality
    - Serialization/deserialization capabilities
    - Business logic for trip calculations
    - Common interface for different trip types
    """
    
    # Common fields across all trip types
    vendor_id: int
    pickup_datetime: datetime
    dropoff_datetime: datetime
    passenger_count: Optional[float]
    trip_distance: Optional[float]
    pickup_location_id: int
    dropoff_location_id: int
    payment_type: int
    fare_amount: Optional[float]
    extra: Optional[float]
    mta_tax: Optional[float]
    tip_amount: Optional[float]
    tolls_amount: Optional[float]
    improvement_surcharge: Optional[float]
    total_amount: Optional[float]
    congestion_surcharge: Optional[float]
    
    # Metadata fields
    trip_type: TripType
    store_and_fwd_flag: Optional[str] = None
    ratecode_id: Optional[float] = None
    
    def __post_init__(self):
        """Validate data after initialization"""
        self._validate_trip_data()
    
    def _validate_trip_data(self) -> None:
        """
        Validate trip data for consistency and business rules
        
        Raises:
            ValueError: If data validation fails
        """
        # Validate datetime fields
        if self.pickup_datetime >= self.dropoff_datetime:
            raise ValueError("Pickup datetime must be before dropoff datetime")
        
        # Validate numeric fields
        if self.trip_distance is not None and self.trip_distance < 0:
            raise ValueError("Trip distance cannot be negative")
        
        if self.passenger_count is not None and self.passenger_count < 0:
            raise ValueError("Passenger count cannot be negative")
        
        # Validate fare amounts
        numeric_fields = [
            'fare_amount', 'extra', 'mta_tax', 'tip_amount', 
            'tolls_amount', 'improvement_surcharge', 'total_amount', 
            'congestion_surcharge'
        ]
        
        for field in numeric_fields:
            value = getattr(self, field)
            if value is not None and value < 0 and field != 'fare_amount':
                # Allow negative fare_amount for refunds, but warn for others
                if field == 'total_amount' and value < -100:  # Extreme negative amounts
                    raise ValueError(f"{field} has extreme negative value: {value}")
    
    @property
    def trip_duration_minutes(self) -> float:
        """Calculate trip duration in minutes"""
        duration = self.dropoff_datetime - self.pickup_datetime
        return duration.total_seconds() / 60
    
    @property
    def trip_duration_hours(self) -> float:
        """Calculate trip duration in hours"""
        return self.trip_duration_minutes / 60
    
    @property
    def average_speed_mph(self) -> Optional[float]:
        """Calculate average speed in miles per hour"""
        if (self.trip_distance is None or 
            self.trip_distance <= 0 or 
            self.trip_duration_hours <= 0):
            return None
        
        return self.trip_distance / self.trip_duration_hours
    
    @property
    def is_valid_trip(self) -> bool:
        """Check if trip passes basic validation rules"""
        try:
            self._validate_trip_data()
            
            # Additional business rule validations
            if self.trip_duration_minutes > 1440:  # More than 24 hours
                return False
            
            if self.average_speed_mph is not None and self.average_speed_mph > 100:  # Unrealistic speed
                return False
            
            return True
            
        except ValueError:
            return False
    
    @property
    def payment_type_name(self) -> str:
        """Get human-readable payment type"""
        try:
            return PaymentType(self.payment_type).name
        except ValueError:
            return "UNKNOWN"
    
    @property
    def rate_code_name(self) -> str:
        """Get human-readable rate code"""
        if self.ratecode_id is None:
            return "UNKNOWN"
        
        try:
            return RateCodeType(int(self.ratecode_id)).name
        except ValueError:
            return "UNKNOWN"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert trip to dictionary for serialization"""
        return {
            'vendor_id': self.vendor_id,
            'pickup_datetime': self.pickup_datetime.isoformat(),
            'dropoff_datetime': self.dropoff_datetime.isoformat(),
            'passenger_count': self.passenger_count,
            'trip_distance': self.trip_distance,
            'pickup_location_id': self.pickup_location_id,
            'dropoff_location_id': self.dropoff_location_id,
            'payment_type': self.payment_type,
            'fare_amount': self.fare_amount,
            'extra': self.extra,
            'mta_tax': self.mta_tax,
            'tip_amount': self.tip_amount,
            'tolls_amount': self.tolls_amount,
            'improvement_surcharge': self.improvement_surcharge,
            'total_amount': self.total_amount,
            'congestion_surcharge': self.congestion_surcharge,
            'trip_type': self.trip_type.value,
            'store_and_fwd_flag': self.store_and_fwd_flag,
            'ratecode_id': self.ratecode_id,
            # Calculated fields
            'trip_duration_minutes': self.trip_duration_minutes,
            'average_speed_mph': self.average_speed_mph,
            'is_valid_trip': self.is_valid_trip
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], trip_type: TripType) -> 'TaxiTrip':
        """Create TaxiTrip from dictionary"""
        return cls(
            vendor_id=data['vendor_id'],
            pickup_datetime=pd.to_datetime(data['pickup_datetime']),
            dropoff_datetime=pd.to_datetime(data['dropoff_datetime']),
            passenger_count=data.get('passenger_count'),
            trip_distance=data.get('trip_distance'),
            pickup_location_id=data['pickup_location_id'],
            dropoff_location_id=data['dropoff_location_id'],
            payment_type=data['payment_type'],
            fare_amount=data.get('fare_amount'),
            extra=data.get('extra'),
            mta_tax=data.get('mta_tax'),
            tip_amount=data.get('tip_amount'),
            tolls_amount=data.get('tolls_amount'),
            improvement_surcharge=data.get('improvement_surcharge'),
            total_amount=data.get('total_amount'),
            congestion_surcharge=data.get('congestion_surcharge'),
            trip_type=trip_type,
            store_and_fwd_flag=data.get('store_and_fwd_flag'),
            ratecode_id=data.get('ratecode_id')
        )


@dataclass
class YellowTaxiTrip(TaxiTrip):
    """Specific model for Yellow Taxi trips"""
    
    def __init__(self, **kwargs):
        # Map yellow taxi specific fields
        if 'tpep_pickup_datetime' in kwargs:
            kwargs['pickup_datetime'] = kwargs.pop('tpep_pickup_datetime')
        if 'tpep_dropoff_datetime' in kwargs:
            kwargs['dropoff_datetime'] = kwargs.pop('tpep_dropoff_datetime')
        if 'PULocationID' in kwargs:
            kwargs['pickup_location_id'] = kwargs.pop('PULocationID')
        if 'DOLocationID' in kwargs:
            kwargs['dropoff_location_id'] = kwargs.pop('DOLocationID')
        if 'VendorID' in kwargs:
            kwargs['vendor_id'] = kwargs.pop('VendorID')
        if 'RatecodeID' in kwargs:
            kwargs['ratecode_id'] = kwargs.pop('RatecodeID')
        
        kwargs['trip_type'] = TripType.YELLOW
        super().__init__(**kwargs)


@dataclass  
class GreenTaxiTrip(TaxiTrip):
    """Specific model for Green Taxi trips"""
    
    # Green taxi specific fields
    ehail_fee: Optional[float] = None
    trip_type_flag: Optional[int] = None  # Different from TripType enum
    
    def __init__(self, **kwargs):
        # Map green taxi specific fields
        if 'lpep_pickup_datetime' in kwargs:
            kwargs['pickup_datetime'] = kwargs.pop('lpep_pickup_datetime')
        if 'lpep_dropoff_datetime' in kwargs:
            kwargs['dropoff_datetime'] = kwargs.pop('lpep_dropoff_datetime')
        if 'PULocationID' in kwargs:
            kwargs['pickup_location_id'] = kwargs.pop('PULocationID')
        if 'DOLocationID' in kwargs:
            kwargs['dropoff_location_id'] = kwargs.pop('DOLocationID')
        if 'VendorID' in kwargs:
            kwargs['vendor_id'] = kwargs.pop('VendorID')
        if 'RatecodeID' in kwargs:
            kwargs['ratecode_id'] = kwargs.pop('RatecodeID')
        if 'trip_type' in kwargs and isinstance(kwargs['trip_type'], int):
            self.trip_type_flag = kwargs.pop('trip_type')
        
        kwargs['trip_type'] = TripType.GREEN
        super().__init__(**kwargs)


class TripDataProcessor:
    """
    Utility class for processing trip data in bulk
    
    Provides methods for:
    - Converting pandas DataFrames to trip objects
    - Batch validation and cleaning
    - Statistical analysis of trip data
    - Data quality reporting
    """
    
    @staticmethod
    def dataframe_to_trips(df: pd.DataFrame, trip_type: TripType) -> List[TaxiTrip]:
        """
        Convert pandas DataFrame to list of TaxiTrip objects
        
        Args:
            df: DataFrame containing trip data
            trip_type: Type of trips in the dataframe
            
        Returns:
            List of TaxiTrip objects
        """
        trips = []
        
        for _, row in df.iterrows():
            try:
                if trip_type == TripType.YELLOW:
                    trip = YellowTaxiTrip(**row.to_dict())
                elif trip_type == TripType.GREEN:
                    trip = GreenTaxiTrip(**row.to_dict())
                else:
                    trip = TaxiTrip.from_dict(row.to_dict(), trip_type)
                
                trips.append(trip)
                
            except Exception as e:
                # Log error but continue processing
                print(f"Failed to create trip from row: {e}")
                continue
        
        return trips
    
    @staticmethod
    def validate_trip_batch(trips: List[TaxiTrip]) -> Dict[str, Any]:
        """
        Validate a batch of trips and return quality metrics
        
        Args:
            trips: List of TaxiTrip objects
            
        Returns:
            Dictionary with validation results and statistics
        """
        total_trips = len(trips)
        valid_trips = sum(1 for trip in trips if trip.is_valid_trip)
        
        # Calculate statistics
        durations = [trip.trip_duration_minutes for trip in trips]
        distances = [trip.trip_distance for trip in trips if trip.trip_distance is not None]
        speeds = [trip.average_speed_mph for trip in trips if trip.average_speed_mph is not None]
        
        return {
            'total_trips': total_trips,
            'valid_trips': valid_trips,
            'invalid_trips': total_trips - valid_trips,
            'validation_rate': valid_trips / total_trips if total_trips > 0 else 0,
            'statistics': {
                'avg_duration_minutes': sum(durations) / len(durations) if durations else 0,
                'avg_distance_miles': sum(distances) / len(distances) if distances else 0,
                'avg_speed_mph': sum(speeds) / len(speeds) if speeds else 0,
                'max_duration_minutes': max(durations) if durations else 0,
                'max_distance_miles': max(distances) if distances else 0,
                'max_speed_mph': max(speeds) if speeds else 0
            }
        }
    
    @staticmethod
    def get_data_quality_report(trips: List[TaxiTrip]) -> Dict[str, Any]:
        """
        Generate comprehensive data quality report
        
        Args:
            trips: List of TaxiTrip objects
            
        Returns:
            Detailed data quality report
        """
        validation_results = TripDataProcessor.validate_trip_batch(trips)
        
        # Analyze common data quality issues
        issues = {
            'zero_distance_trips': sum(1 for trip in trips if trip.trip_distance == 0),
            'zero_fare_trips': sum(1 for trip in trips if trip.fare_amount == 0),
            'negative_amounts': sum(1 for trip in trips if trip.total_amount is not None and trip.total_amount < 0),
            'extreme_speeds': sum(1 for trip in trips if trip.average_speed_mph is not None and trip.average_speed_mph > 80),
            'long_duration_trips': sum(1 for trip in trips if trip.trip_duration_minutes > 180),  # > 3 hours
        }
        
        return {
            **validation_results,
            'data_quality_issues': issues,
            'recommendations': TripDataProcessor._generate_recommendations(validation_results, issues)
        }
    
    @staticmethod
    def _generate_recommendations(validation_results: Dict[str, Any], issues: Dict[str, int]) -> List[str]:
        """Generate recommendations based on data quality analysis"""
        recommendations = []
        
        if validation_results['validation_rate'] < 0.95:
            recommendations.append("Consider implementing stricter data validation rules")
        
        if issues['zero_distance_trips'] > validation_results['total_trips'] * 0.05:
            recommendations.append("High number of zero-distance trips detected - review pickup/dropoff logic")
        
        if issues['extreme_speeds'] > validation_results['total_trips'] * 0.01:
            recommendations.append("Extreme speeds detected - validate GPS coordinates and timestamps")
        
        if issues['negative_amounts'] > 0:
            recommendations.append("Negative fare amounts found - implement refund/adjustment tracking")
        
        return recommendations