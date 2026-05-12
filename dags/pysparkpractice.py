import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Employee Data Analysis") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Users/mtq/airflow-docker/dags/employees")

df.show()
df.printSchema()

spark.stop()