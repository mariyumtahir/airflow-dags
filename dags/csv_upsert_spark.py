import os
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowSkipException

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    row_number,
    regexp_extract,
    input_file_name
)

from pyspark.sql.window import Window


# PATHS

DATA_DIR = "/data"
DESTINATION_DIR = "/output/final_data"



# MAIN FUNCTION

def process_partition(**context):

    
    # STEP 1: CURRENT PARTITION IDENTIFICATION
    

    data_interval_start = context["data_interval_start"]

    folder_name = data_interval_start.strftime("%Y-%m-%d-%H")

    partition_path = f"{DATA_DIR}/time_stamp={folder_name}"

    print(f"[INFO] Reading partition: {partition_path}")

    # STEP 2: CHECK FOLDER EXISTS

    if not os.path.isdir(partition_path):

        raise AirflowSkipException(
            f"Partition not found: {partition_path}"
        )

    # STEP 3: CREATE SPARK SESSION
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("csv_upsert_pyspark") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:

        # STEP 4: READ ALL FILES FROM CURRENT PARTITION
        

        df_new = spark.read.csv(
            partition_path,
            header=True,
            inferSchema=True
        )

        print(f"[INFO] Total incoming rows: {df_new.count()}")

        
        # STEP 5: FILE INDEX EXTRACT KARO

        df_new = df_new.withColumn(

            "file_index",

            regexp_extract(
                input_file_name(),
                r"data_(\d+)\.csv",
                1
            ).cast("int")
        )

        # STEP 6: LATEST RECORD PER ID

        window_spec = Window \
            .partitionBy("id") \
            .orderBy(col("file_index").desc())

        df_latest = df_new \
            .withColumn(
                "rank",
                row_number().over(window_spec)
            ) \
            .filter(col("rank") == 1) \
            .drop("rank", "file_index")

        print(f"[INFO] Latest unique rows: {df_latest.count()}")

        # STEP 7: FIRST ITERATION
       
        if not os.path.exists(DESTINATION_DIR):

            print("[INFO] First run detected")

            df_latest.write \
                .mode("overwrite") \
                .option("header", True) \
                .csv(DESTINATION_DIR)

            print("[INFO] Initial load completed")

        
        # STEP 8: NEXT ITERATIONS
       
        else:

            print("[INFO] Existing destination found")

            # READ EXISTING DATA
            

            df_existing = spark.read.csv(
                DESTINATION_DIR,
                header=True,
                inferSchema=True
            )

            print(f"[INFO] Existing rows: {df_existing.count()}")

            # REMOVE OLD VERSIONS
           
            df_existing_filtered = df_existing.join(

                df_latest.select("id"),

                on="id",

                how="left_anti"
            )

            print(
                f"[INFO] Remaining old rows: "
                f"{df_existing_filtered.count()}"
            )

            
            # MERGE OLD + NEW
            

            df_final = df_existing_filtered.union(df_latest)

            print(f"[INFO] Final merged rows: {df_final.count()}")

            
            # WRITE FINAL DATA
            

            df_final.write \
                .mode("overwrite") \
                .option("header", True) \
                .csv(DESTINATION_DIR)

            print("[INFO] Upsert completed")

    finally:

        
        # STOP SPARK SESSION

        spark.stop()

# DAG DEFINITION


with DAG(

    dag_id="pyspark_upsert_pipeline",

    start_date=datetime(2026, 5, 1, 0, 0, 0),

    end_date=datetime(2026, 5, 1, 4, 0, 0),

    schedule="@hourly",

    catchup=True,

    tags=["spark", "upsert", "incremental"]

) as dag:

    process_task = PythonOperator(

        task_id="process_partition",

        python_callable=process_partition
    )

    process_task