# NYC Taxi Data Ingestion Pipeline

A production-ready, scalable data ingestion pipeline for NYC Taxi & Limousine Commission (TLC) trip data. This project demonstrates modern data engineering practices using Python, Snowflake, and AWS S3.

## 🚀 Features

- **Scalable Architecture**: Modular design with clear separation of concerns
- **Cloud-Native**: Integrates with Snowflake and AWS S3 for external staging
- **Parallel Processing**: Configurable multi-threading for faster data processing
- **Data Quality**: Comprehensive validation and quality metrics
- **Error Handling**: Robust error handling with retry logic and graceful degradation
- **Monitoring**: Structured logging and performance metrics
- **Configurable**: Environment-based configuration management
- **Production-Ready**: Docker support, comprehensive testing, and CI/CD ready

## 🏗 Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   TLC Data      │    │   File           │    │   Stage         │
│   Source        ├───►│   Extractor      ├───►│   Manager       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐             │
│   Snowflake     │◄───│   Snowflake      │◄────────────┘
│   Tables        │    │   Loader         │
└─────────────────┘    └──────────────────┘
```

## 📋 Prerequisites

- Python 3.9+
- Snowflake account with appropriate permissions
- AWS account with S3 access (for external staging)
- At least 4GB of available disk space for temporary files

## 🛠 Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd nyc_taxi_ingestion
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your actual configuration values
   ```

## ⚙️ Configuration

The pipeline uses environment variables for configuration. Copy `.env.example` to `.env` and configure:

### Required Settings

- **Snowflake**: Account, username, password, warehouse, database, schema
- **AWS S3**: Access key, secret key, bucket name, region
- **Pipeline**: Data directory, batch size, worker count

### Optional Settings

- **Logging**: Log level, log directory
- **TLC Source**: Custom base URL, retry settings, timeouts
- **Processing**: Enable/disable validation, cleanup settings

## 🚀 Usage

### Command Line Interface

The pipeline provides a comprehensive CLI for various operations:

```bash
# Ingest recent 3 months of yellow taxi data
python scripts/run_ingestion.py --trip-type yellow_tripdata --months-back 3

# Ingest specific date range
python scripts/run_ingestion.py --trip-type green_tripdata --date-range 2024-01 2024-03

# Run with debug logging
python scripts/run_ingestion.py --trip-type yellow_tripdata --log-level DEBUG

# Use direct loading (skip external staging)
python scripts/run_ingestion.py --trip-type yellow_tripdata --no-staging

# Check pipeline status
python scripts/run_ingestion.py --status

# Clean up temporary resources
python scripts/run_ingestion.py --cleanup

# Dry run to see what would be processed
python scripts/run_ingestion.py --trip-type yellow_tripdata --dry-run
```

### Programmatic Usage

```python
from src.orchestrator.ingestion_pipeline import IngestionPipeline

# Initialize pipeline
pipeline = IngestionPipeline()

# Ingest recent data
result = pipeline.ingest_recent_data(
    trip_type="yellow_tripdata",
    months_back=3,
    use_external_stage=True
)

print(f"Processed {result.files_processed} files with {result.total_records} records")
```

## 📊 Data Processing Flow

1. **Discovery**: Identifies available TLC data files based on date range
2. **Extraction**: Downloads Parquet files with retry logic and progress tracking
3. **Staging**: Uploads files to S3 and creates Snowflake external stages (optional)
4. **Loading**: Bulk loads data into Snowflake raw tables with validation
5. **Quality**: Validates data quality and generates metrics
6. **Cleanup**: Removes temporary files and manages storage

## 🏢 Snowflake Schema

The pipeline creates raw tables with the following structure:

```sql
-- Yellow Taxi Trips
CREATE TABLE raw_yellow_tripdata (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    -- ... other TLC fields
    
    -- Metadata columns
    _file_name VARCHAR(255),
    _load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _record_hash VARCHAR(64)
);
```

## 📈 Monitoring & Observability

- **Structured Logging**: JSON format logs for easy parsing
- **Performance Metrics**: Processing times, throughput, error rates
- **Data Quality Metrics**: Validation results, completeness, consistency
- **Health Checks**: Pipeline status and connectivity verification

## 🧪 Testing

```bash
# Run unit tests
pytest tests/unit/

# Run integration tests (requires valid configuration)
pytest tests/integration/

# Generate coverage report
pytest --cov=src tests/
```

## 🐳 Docker Support

```bash
# Build Docker image
docker build -t nyc-taxi-pipeline .

# Run pipeline
docker run --env-file .env nyc-taxi-pipeline --trip-type yellow_tripdata --months-back 1
```

## 📝 Data Sources

This pipeline processes NYC TLC trip record data:

- **Yellow Taxi**: Traditional yellow taxi trips
- **Green Taxi**: Green taxi trips (primarily outer boroughs)
- **Data Format**: Parquet files with monthly partitioning
- **Update Frequency**: Monthly with ~2 month publication delay
- **Data Dictionary**: [NYC TLC Trip Record Data Dictionary](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## 🚨 Error Handling

- **Retry Logic**: Exponential backoff for network operations
- **Graceful Degradation**: Continues processing other files if one fails
- **Error Collection**: Aggregates errors for batch reporting
- **Data Validation**: Comprehensive quality checks with warnings/errors
- **Rollback Support**: Transaction-based loading with error recovery

## 🔒 Security Considerations

- **Credentials**: Use environment variables or secrets management
- **Network**: Supports VPC endpoints and private networking
- **Encryption**: S3 server-side encryption enabled by default
- **Access Control**: Principle of least privilege for all services

## 📊 Performance Characteristics

- **Throughput**: ~50,000 records/second on modest hardware
- **Scalability**: Horizontal scaling via increased worker threads
- **Memory**: Streaming processing to handle large files efficiently
- **Storage**: Temporary files cleaned up automatically

## 🛣 Roadmap

- [ ] Support for FHV and HVFHV trip types
- [ ] Delta Lake integration for data versioning
- [ ] Apache Airflow DAG templates
- [ ] Real-time streaming ingestion
- [ ] Data quality monitoring dashboard
- [ ] Integration with dbt 
