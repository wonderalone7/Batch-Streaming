from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import time
import glob
from pathlib import Path

# Define schema for your CSV
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("value", DoubleType(), True),
])

def create_spark_session():
    """Create a new Spark session for each batch"""
    return (
        SparkSession.builder
        .appName("Batch CSV Processor")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "512m")
        .getOrCreate()
    )

def process_batch(csv_file, batch_num):
    """Process a single CSV file"""
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        print(f"\n{'='*80}")
        print(f"Batch {batch_num}: Processing {Path(csv_file).name}")
        print(f"{'='*80}\n")
        
        # Read CSV
        df = spark.read.csv(
            csv_file,
            header=True,
            schema=schema
        )
        
        # Process
        processed_df = df.select(
            col("id"),
            col("name"),
            col("value"),
            (col("value") * 1.1).alias("value_with_tax"),
            current_timestamp().alias("processed_at")
        )
        
        # Display results
        processed_df.show(truncate=False)
        
        # Optionally write to a CSV output file
        output_path = f"/app/output/batch_{batch_num}_result.csv"
        os.makedirs("/app/output", exist_ok=True)
        processed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"\nOutput saved to: {output_path}\n")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"ERROR processing {csv_file}: {e}")
        return False

# ============================================================================
# NOTE: For Airflow integration, use only the functions above
# DO NOT use this file to run directly - instead, use the Airflow DAG:
#   dags/streaming_batch_dag.py
#
# The functions in this file are called by the Airflow DAG
# ============================================================================

if __name__ == "__main__":
    # For local testing ONLY
    print("="*80)
    print("🚀 LOCAL TESTING MODE")
    print("="*80)
    print("\nFor production use, deploy the Airflow DAG:")
    print("  → dags/streaming_batch_dag.py")
    print("\nThis script is now a LIBRARY of functions used by Airflow.")
    print("\nTo test locally, use:")
    print("  python -c \"from src.streaming_app import process_batch; process_batch('path/to/file.csv', 1)\"")
    print("="*80)