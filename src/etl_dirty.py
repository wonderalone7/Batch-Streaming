from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,lower,trim,regexp_replace,when,count
)

spark = (
    SparkSession.builder
    .appName("Dirty CSV ETL Pipeline")
    .master("local[*]")
    .getOrCreate()
)
# 2. READ DIRTY CSV FILE

raw_df = (
    spark.read
    .option("header", True)
    .csv(
        "C:\\Users\\Gnyandeep\\Downloads\\archive\\uber_rapido_driver_dirty.csv"
    )
)

clean_df = raw_df

string_columns = [
    field.name
    for field in clean_df.schema.fields
    if field.dataType.simpleString() == "string"
]
#Standardize string columns: trim, lower case, replace multiple spaces with single space
for column in string_columns:

    clean_df = clean_df.withColumn(

        column,
        lower(
            trim(
                regexp_replace(
                    col(column),
                    r"\s+",
                    " "
                )
            )
        )
    )

dirty_values = [
    "na",
    "n/a",
    "null",
    "none",
    "unknown",
    ""
]

clean_df = clean_df.replace(dirty_values, None)

null_df = clean_df.select([

    count(
        when(col(c).isNull(), c)
    ).alias(c)
    for c in clean_df.columns
])

#clean_df.select("platform").distinct().show()

#null_df.show(vertical=True)
#print("Total rows before duplicate removal:", clean_df.count())
clean_df = clean_df.dropDuplicates()
#print("Total rows after duplicate removal:", clean_df.count())
#clean_df.printSchema()

clean_df = clean_df.withColumn(
    "age",
    regexp_replace(col("age"), " yrs", "")
)
clean_df = clean_df.withColumn(
    "age",
    col("age").cast("integer")
)
clean_df = clean_df.withColumn(
    "age",
    when(
        (col("age") < 18) & (col("age") > 65),
        col("age")
    ).otherwise(None)
)