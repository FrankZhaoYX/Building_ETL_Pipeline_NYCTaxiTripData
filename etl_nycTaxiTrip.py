
import os
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ['JAVA_HOME'] = r'E:\Master\University_Of_Waterloo\CS647\A1\openlogic-openjdk-8u402-b06-windows-x64\openlogic-openjdk-8u402-b06-windows-64'
DATA_PATH='E:\Github\Building_ETL_Pipeline_NYCTaxiTripData\data'
BRONZE_PATH_PREFEX = 'E:\Github\Building_ETL_Pipeline_NYCTaxiTripData\bronze'

def create_spark_session():
    """Create Spark session with proper configuration"""
    os.environ['HADOOP_HOME'] = os.path.dirname(os.path.abspath(__file__))
    spark = SparkSession.builder \
        .appName("NYC Taxi ETL") \
        .master("local[*]") \
        .getOrCreate()   
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def scan_parquet_files():
    parquet_files = []
    def scan_directory(directory_path):
            try:
                with os.scandir(directory_path) as entries:
                    for entry in entries:
                        if entry.is_file() and entry.name.endswith('parquet'):
                            parquet_files.append(entry.path)
                        elif entry.is_dir():
                            scan_directory(entry.path)
            except FileNotFoundError:
                 print(f"Directory '{directory_path} not FOUND")
    scan_directory(DATA_PATH)
    return parquet_files

def extract_data(dataframe):
    df = dataframe.dropna()
    df = df.rename(columns={'tpep_pickup_datetime': 'pickup_datetime', 'tpep_dropoff_datetime': 'dropoff_datetime'})
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
    df['trip_duration'] = df['dropoff_datetime'] - df['pickup_datetime']
    df['trip_duration_minutes'] = df['trip_duration'].dt.total_seconds() / 60

# def load_data(dataframe):


if __name__=="__main__":
    parquet_files = scan_parquet_files()
    
    # Create Spark session
    spark = create_spark_session()
    # Extract data from local
    for file in parquet_files:
        print(file)
        df= spark.read.parquet(file)
        df = extract_data(df)

        
    
    spark.stop()