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

def main():
    """Monitor and process CSV files"""
    input_dir = "/app/streaming_input"
    processed_dir = "/app/processed_files"
    os.makedirs(processed_dir, exist_ok=True)
    
    batch_num = 0
    processed_files = set()
    
    print("="*80)
    print("Real-Time Batch Processing Started")
    print("Monitoring: /app/streaming_input")
    print("Output: /app/output")
    print("="*80)
    
    try:
        while True:
            # Find all CSV files
            csv_files = sorted(glob.glob(f"{input_dir}/*.csv"))
            
            # Process new files
            for csv_file in csv_files:
                if csv_file not in processed_files:
                    if process_batch(csv_file, batch_num):
                        processed_files.add(csv_file)
                        batch_num += 1
                        
                        # Move processed file to archive
                        import shutil
                        archived_file = os.path.join(processed_dir, Path(csv_file).name)
                        shutil.move(csv_file, archived_file)
                        print(f"Archived: {csv_file}\n")
            
            # Wait before checking again
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")

if __name__ == "__main__":
    main()