# NYC Taxi Data ETL Pipeline

## Step 1: Extract, Transform, and Load (ETL) to Bronze Dataset

### 📊 Data Source
- **Source**: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Format**: Parquet files
- **Time Period**: January 2009 - December 2009 (12 months)
- **Dataset**: Yellow Taxi Trip Data

### 🔄 Data Processing Workflow

#### **Extract Phase**
- **Input**: NYC Yellow Taxi parquet files (`yellow_tripdata_2009-01.parquet` to `yellow_tripdata_2009-12.parquet`)
- **Location**: `data/yellow_tripdata_2009/` directory
- **Processing Tool**: PySpark for distributed data processing
- **File Discovery**: Recursive scanning to locate all parquet files


#### **Transform Phase**
1. **Data Cleaning**
  - Remove rows with null/missing values using `dropna()`
  - Handle data quality issues

  2. **Derived Field Creation**
  - **`trip_duration`**: Time difference between dropoff and pickup (in seconds)
  - **`trip_duration_minutes`**: Trip duration converted to minutes for analysis
  - **`data_date`**: Extract YYYY-MM format from filename (e.g., "2009-01") for partitioning

  3. **Data Enrichment**
  - Add metadata columns for data lineage and processing tracking
  - Maintain original data integrity while adding analytical fields

  #### **Load Phase**
- **Output Format**: CSV files for downstream processing
- **Destination**: `bronze/yellow_tripdata.csv` (consolidated file)
- **Loading Strategy**: Append mode - combines all monthly files into single dataset
- **Data Validation**: Column compatibility checks before appending

### 🛠️ Technical Implementation
#### **Technologies Used**
- **PySpark**: Distributed data processing and transformations
- **Pandas**: Data manipulation and CSV operations
- **Python**: ETL orchestration and file handling

#### **Key Features**
- **Scalable Processing**: Handles large parquet files efficiently
- **Error Handling**: Robust error management with detailed logging
- **Data Validation**: Automatic column detection and compatibility checks
- **Progress Tracking**: Real-time processing status and statistics
- **Memory Optimization**: Efficient DataFrame operations and cleanup

### 📁 File Structure
<pre>
project/
├── data/
│   └── yellow_tripdata_2009/
│       ├── yellow_tripdata_2009-01.parquet
│       ├── yellow_tripdata_2009-02.parquet
│       └── ... (through 2009-12)
├── bronze/
│   └── yellow_tripdata.csv
└── etl_nycTaxiTrip.py
</pre>

### 📈 Data Schema

#### **Input Schema** (Original Parquet)
- `Trip_Pickup_DateTime`: Pickup timestamp
- `Trip_Dropoff_DateTime`: Dropoff timestamp
- `Fare_Amt`: Trip fare amount
- `Passenger_Count`: Number of passengers
- `Trip_Distance`: Distance traveled
- *[Other original TLC fields]*

#### **Output Schema** (Bronze CSV)
- `trip_duration`: Trip duration in seconds
- `trip_duration_minutes`: Trip duration in minutes
- `data_date`: Source file date (YYYY-MM)
- *[All original fields preserved]*

### 🎯 Success Metrics
- **Data Volume**: ~14M+ records per month, 150M+ total records processed
- **Data Quality**: 100% schema compliance after transformations
- **Processing Speed**: Optimized for large-scale data processing
- **Data Lineage**: Full traceability with source file metadata

### 🔍 Quality Assurance, Data Qulity check
- **Schema Validation**: Automatic detection of column mismatches
- **Data Integrity**: Preservation of all original data fields
- **Error Logging**: Comprehensive error reporting and recovery
- **Sample Verification**: Display of processed data samples for validation

### ⚡ Performance Optimizations
- **Spark Configuration**: Optimized memory usage and parallel processing
- **Adaptive Query Execution**: Dynamic optimization of Spark operations
- **Incremental Loading**: Append-only strategy for efficient data accumulation
- **Resource Management**: Proper cleanup and memory management

---

## Next Steps
The bronze dataset serves as the foundation for:
- **Step 2**: Data quality assessment and silver layer transformations
- **Step 3**: Business logic application and gold layer aggregations
- **Step 4**: Analytics and reporting layer preparation

> **Note**: This ETL pipeline is designed for scalability and can be extended to process additional years or taxi types (green, FHV) with minimal modifications.

