import os
os.environ["JAVA_HOME"] = "/usr/local/Cellar/openjdk@11/11.0.30/libexec/openjdk.jdk/Contents/Home"
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.exceptions import AirflowSkipException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, regexp_extract, input_file_name
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

# VARIABLES

DATA_DIR   = "/Users/mtq/airflow-docker/data"
OUTPUT_DIR = "/Users/mtq/airflow-docker/output"
OUTPUT_CSV = "/Users/mtq/airflow-docker/output/result.csv"

COLUMNS = ["id", "name", "age", "department", "salary", "working_hours"]

# MAIN FUNCTION

def process_partition(**context):

    # === STEP 1: LOGICAL INTERVAL ===

    data_interval_start = context["data_interval_start"]
    data_interval_end   = context["data_interval_end"]

    print(f"[INFO] data_interval_start : {data_interval_start}")
    print(f"[INFO] data_interval_end   : {data_interval_end}")

    folder_name    = data_interval_start.strftime("%Y-%m-%d-%H")
    partition_path = f"{DATA_DIR}/time_stamp={folder_name}"

    print(f"[INFO] Processing partition: {partition_path}")

    # === STEP 2: FOLDER CHECK ===

    if not os.path.isdir(partition_path):
        raise AirflowSkipException(f"Folder don't exist: {partition_path}")

    # === STEP 3: SPARK SESSION ===

    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("csv_upsert_pipeline") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # === STEP 4: READ ALL CSVs ===

    df_new = spark.read.csv(
        partition_path,
        header=True,
        inferSchema=True
    )

    print(f"[INFO] Total rows read: {df_new.count()}")

    # === STEP 5: FILE INDEX COLUMN ADD KARO ===

    df_new = df_new.withColumn(
        "file_index",
        regexp_extract(input_file_name(), r"file_(\d+)\.csv", 1).cast("int")
    )

    # === STEP 6: LATEST RECORD PER ID ===

    window = Window.partitionBy("id").orderBy(col("file_index").desc())

    df_resolved = df_new \
        .withColumn("rank", row_number().over(window)) \
        .filter(col("rank") == 1) \
        .drop("rank", "file_index")

    print(f"[INFO] Resolved {df_resolved.count()} unique IDs.")

    # === STEP 7: UPSERT ===

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    if not os.path.isdir(OUTPUT_CSV):

        # FIRST RUN
        print("[INFO] result.csv not found — create new.")

        df_resolved \
            .coalesce(1) \
            .write.csv(OUTPUT_CSV, header=True, mode="overwrite")

        print(f"[INFO] result.csv created with {df_resolved.count()} rows.")

    else:

        # SUBSEQUENT RUNS
        print("[INFO] result.csv found — upsert.")

        SCHEMA = StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("age", StringType()),
            StructField("department", StringType()),
            StructField("salary", StringType()),
            StructField("working_hours", StringType()),
        ])
        
        df_existing = spark.read.csv(
            OUTPUT_CSV,
            header=True,
            inferSchema=True,
            schema=SCHEMA,
        )

        print(f"[INFO] Existing rows: {df_existing.count()}")

        # EXISTING MEIN SE NEW IDs HATA DO
        df_existing_filtered = df_existing.join(
            df_resolved.select("id"),
            on="id",
            how="left_anti"
        )

        # DONO MERGE KARO
        df_final = df_existing_filtered.union(df_resolved)

        df_final \
            .coalesce(1) \
            .write.csv(OUTPUT_CSV, header=True, mode="overwrite")

        print(f"[INFO] result.csv updated — total {df_final.count()} rows.")

    spark.stop()


# === DAG DEFINITION ===

with DAG(
    dag_id     = "csv_upsert_pipeline",
    start_date = datetime(2026, 5, 1, 0, 0, 0),
    end_date   = datetime(2026, 5, 1, 4, 0, 0),
    schedule   = "@hourly",
    catchup    = True,
    tags       = ["csv", "upsert", "incremental"],
) as dag:

    process_task = PythonOperator(
        task_id         = "process_partition",
        python_callable = process_partition,
    )
