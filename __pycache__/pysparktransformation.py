import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import max

spark = SparkSession.builder \
    .appName("Employee Data Analysis") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("employees.csv")

#1. Total number of employees
df.count()
print("Total Employees:", df.count())

#2. How many employees joined after 2021
df.filter(df["joining_date"] > "2021-12-31").count()
print("Employees joined after 2021:", df.filter(df["joining_date"] > "2021-12-31").count())

#3. 3 most senior employees
print("3 Most Senior Employees:")
df.orderBy(df["joining_date"].asc()).limit(3).show()

#4. How many employees in each city
print("Employees in each city:")
df.groupBy("city").count().show()

#5. How many employees in each department
print("Employees in each department:")
df.groupBy("department").count().orderBy("count", ascending=False).show()

#6. Highest salary in each department
print("Highest salary in each department:")
df.groupBy("department").agg(max("salary").alias("highest_salary")).orderBy("highest_salary", ascending=False).show()

#7. Employee name and salary with highest salary in each department
dept_max = df.groupBy("department").agg(max("salary").alias("max_salary"))

result = df.join(dept_max, on="department") \
           .filter(df["salary"] == dept_max["max_salary"]) \
           .select("department", "name", "salary") \
           .orderBy("salary", ascending=False)

print("Top earner in each department:")
result.show()