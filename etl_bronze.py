import os
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re

os.environ['JAVA_HOME'] = r'E:\Master\University_Of_Waterloo\CS647\A1\openlogic-openjdk-8u402-b06-windows-x64\openlogic-openjdk-8u402-b06-windows-64'
DATA_PATH = r'E:\Github\Building_ETL_Pipeline_NYCTaxiTripData\data\yellow_tripdata_2009'
BRONZE_PATH_PREFIX = r'E:\Github\Building_ETL_Pipeline_NYCTaxiTripData\bronze'
OUTPUT_CSV_PATH = os.path.join(BRONZE_PATH_PREFIX, 'yellow_tripdata.csv')


def create_spark_session():
    """Create Spark session with proper configuration"""
    os.environ['HADOOP_HOME'] = os.path.dirname(os.path.abspath(__file__))
    
    # Calculate available memory (leave 2GB for OS)
    import psutil
    import builtins
    available_memory = psutil.virtual_memory().total // (1024**3) - 2  # GB
    driver_memory = builtins.min(builtins.max(4, available_memory // 2), 16)  # Between 4-16 GB
    
    spark = SparkSession.builder \
        .appName("NYC Taxi ETL") \
        .master("local[*]") \
        .config("spark.driver.memory", f"{driver_memory}g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print(f"💾 Spark configured with {driver_memory}GB driver memory")
    return spark


def scan_parquet_files():
    """Recursively scan for all parquet files in the data directory"""
    parquet_files = []
    def scan_directory(directory_path):
        try:
            with os.scandir(directory_path) as entries:
                for entry in entries:
                    if entry.is_file() and entry.name.endswith('.parquet'):
                        parquet_files.append(entry.path)
                    elif entry.is_dir():
                        scan_directory(entry.path)
        except FileNotFoundError:
            print(f"❌ Directory '{directory_path}' not FOUND")
    
    scan_directory(DATA_PATH)
    print(f"📁 Found {len(parquet_files)} parquet files to process")
    return sorted(parquet_files)  # Sort for consistent processing order


def extract_date_from_filename(file_path):
    """Extract YYYY-MM from filename like 'yellow_tripdata_2009-01.parquet'"""
    filename = os.path.basename(file_path)
    pattern = r'(\d{4}-\d{2})'
    match = re.search(pattern, filename)
    return match.group(1) if match else None


def extract_data(df, file):
    """Transform the dataframe: rename columns, add date column"""
    date_string = extract_date_from_filename(file)
    
    # Transformations
    df = df.withColumn('Trip_pickup_datetime', to_timestamp(col('Trip_pickup_datetime'))) \
           .withColumn('Trip_dropoff_datetime', to_timestamp(col('Trip_dropoff_datetime'))) \
           .withColumn('trip_duration_seconds', 
                      unix_timestamp('Trip_dropoff_datetime') - unix_timestamp('Trip_pickup_datetime')) \
           .withColumn('trip_duration_minutes', col('trip_duration_seconds') / 60) \
           .withColumn('trip_duration', col('trip_duration_seconds')) \
           .withColumn('data_date', lit(date_string))
    
    print(f"📊 Processed {df.count():,} rows from {os.path.basename(file)}")
    return df


def load_data(dataframe, file_name):
    """Load dataframe to CSV, either creating new or appending to existing"""
    os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)
    
    # Check if file exists
    if os.path.exists(OUTPUT_CSV_PATH):
        print(f"📁 File exists: {os.path.basename(OUTPUT_CSV_PATH)}")
        print(f"➕ Appending {len(dataframe):,} rows from {os.path.basename(file_name)}...")
        
        # Read current file size for reporting
        try:
            # Only read first row to get row count efficiently
            existing_rows = sum(1 for line in open(OUTPUT_CSV_PATH)) - 1  # Subtract header
            print(f"📊 Current file has {existing_rows:,} rows")
        except:
            existing_rows = 0
        
        # Append without header
        dataframe.to_csv(OUTPUT_CSV_PATH, mode='a', header=False, index=False)
        
        total_rows = existing_rows + len(dataframe)
        print(f"✅ Successfully appended! Total rows now: {total_rows:,}")
        
    else:
        print(f"📝 File doesn't exist: {os.path.basename(OUTPUT_CSV_PATH)}")
        print(f"🆕 Creating new CSV with {len(dataframe):,} rows...")
        
        # Write with header
        dataframe.to_csv(OUTPUT_CSV_PATH, mode='w', header=True, index=False)
        print(f"✅ Successfully created: {OUTPUT_CSV_PATH}")


def validate_columns(dataframe):
    """
    Data Quality check - ensure column consistency
    """
    if not os.path.exists(OUTPUT_CSV_PATH):
        return True  # No existing file to validate against
    
    try:
        # Read only the header of existing file
        existing_df = pd.read_csv(OUTPUT_CSV_PATH, nrows=0)
        existing_columns = existing_df.columns.tolist()
        new_columns = dataframe.columns.tolist()
        
        if existing_columns != new_columns:
            print("⚠️  Column mismatch detected!")
            print(f"Existing columns: {existing_columns}")
            print(f"New columns: {new_columns}")
            
            # Show differences
            missing_in_new = set(existing_columns) - set(new_columns)
            extra_in_new = set(new_columns) - set(existing_columns)
            
            if missing_in_new:
                print(f"❌ Missing in new data: {list(missing_in_new)}")
            if extra_in_new:
                print(f"➕ Extra in new data: {list(extra_in_new)}")
            
            return False
        
        return True
        
    except Exception as e:
        print(f"⚠️  Error validating columns: {e}")
        return False


def main():
    """Main ETL pipeline execution"""
    print("🚀 Starting NYC Taxi ETL Pipeline")
    print(f"📂 Data Path: {DATA_PATH}")
    print(f"📂 Output Path: {OUTPUT_CSV_PATH}")
    print("-" * 50)
    
    # Scan for parquet files
    parquet_files = scan_parquet_files()
    
    if not parquet_files:
        print("❌ No parquet files found to process!")
        return
    
    # Create Spark session
    spark = create_spark_session()
    
    # Process statistics
    successful_files = 0
    failed_files = 0
    total_rows_processed = 0
    
    try:
        # Process each parquet file
        for i, file in enumerate(parquet_files, 1):
            print(f"\n📄 Processing file {i}/{len(parquet_files)}: {os.path.basename(file)}")
            print("-" * 30)
            
            try:
                # Read parquet file
                df = spark.read.parquet(file)
                
                # Extract and transform data
                df = extract_data(df, file)
                
                # Convert to Pandas DataFrame
                pandas_df = df.toPandas()
                total_rows_processed += len(pandas_df)
                
                # Validate and load data
                if validate_columns(pandas_df):
                    load_data(pandas_df, file)
                    successful_files += 1
                else:
                    print(f"❌ File {os.path.basename(file)} failed validation - skipping")
                    failed_files += 1
                    
            except Exception as e:
                print(f"❌ Error processing {os.path.basename(file)}: {str(e)}")
                failed_files += 1
                continue
        
        # Print summary
        print("\n" + "=" * 50)
        print("📊 ETL Pipeline Summary")
        print("=" * 50)
        print(f"✅ Successfully processed: {successful_files} files")
        print(f"❌ Failed: {failed_files} files")
        print(f"📈 Total rows processed: {total_rows_processed:,}")
        print(f"💾 Output saved to: {OUTPUT_CSV_PATH}")
        
    finally:
        # Always stop Spark session
        spark.stop()
        print("\n🏁 ETL Pipeline completed!")


if __name__ == "__main__":
    main()