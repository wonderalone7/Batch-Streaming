from pyspark.sql import SparkSession
from sqlalchemy import column
spark= SparkSession.builder.appName("name of your app").getOrCreate()

df = spark.read.csv("C:\\Users\\Gnyandeep\\Downloads\\cleaned_cafe_sales.csv", header=True, inferSchema=True)

distinct_df = df.select("location").distinct()
distinct_df.show()