from pyspark.sql import SparkSession
import json

spark = (
    SparkSession.builder
    .appName("Cafe Sales ETL Pipeline")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Step 1: EXTRACT - Reading CSV file...")
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("C:\\Users\\Gnyandeep\\Downloads\\cleaned_cafe_sales.csv")
)

total_rows = df.count()
total_cols = len(df.columns)

print(f"✓ Data loaded successfully!")
print(f"  └─ Rows: {total_rows}")
print(f"  └─ Columns: {total_cols}")

print("\n===== SCHEMA =====")
df.printSchema()

print("\n\nStep 2: Converting to JSON format...")

# Convert to JSON (as a list of dictionaries)
json_data = df.toJSON().map(lambda x: json.loads(x)).collect()

# Create output JSON file
output_json = "C:\\Users\\Gnyandeep\\Downloads\\cleaned_cafe_sales.json"
with open(output_json, 'w') as f:
    json.dump(json_data, f, indent=2)

print(f"✓ JSON file created at: {output_json}")


print(f"\n===== FIRST 3 RECORDS (JSON FORMAT) =====")
for i, record in enumerate(json_data[:3], 1):
    print(f"\nRecord {i}:")
    print(json.dumps(record, indent=2))

print(f"\n\n===== SUMMARY =====")
print(f"Total records: {total_rows}")
print(f"Total columns: {total_cols}")
print(f"Output file: {output_json}")
print("\n===== ETL PIPELINE COMPLETED SUCCESSFULLY =====")