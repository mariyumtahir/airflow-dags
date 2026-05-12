import os
import csv
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from croniter import croniter

# VARIABLES

DATA_DIR   = "/data"
OUTPUT_DIR = "/output"
OUTPUT_CSV = "/output/result.csv"

COLUMNS = ["id", "name", "age", "department", "salary", "working_hours"]


# MAIN FUNCTION

def process_partition(**context):
    
    # === TASK 2: LOGICAL INTERVAL ===

    data_interval_start = context["data_interval_start"]
    data_interval_end   = context["data_interval_end"]

    print(f"[INFO] data_interval_start : {data_interval_start}")
    print(f"[INFO] data_interval_end   : {data_interval_end}")

    # FOLDER NAME FORMAT: YYYY-MM-DD-HH 

    folder_name    = data_interval_start.strftime("%Y-%m-%d-%H")
    partition_path = f"{DATA_DIR}/time_stamp={folder_name}"

    print(f"[INFO] Processing partition: {partition_path}")

    

    # === TASK 3: READ All CSVs ===
    
    if not os.path.isdir(partition_path):
        raise AirflowSkipException(f"Folder don't exist: {partition_path}")


    # ONLY CSV FILES
    all_files = [f for f in os.listdir(partition_path) if f.endswith(".csv")]

    if not all_files:
        raise FileNotFoundError(f"CSV not found in: {partition_path}")

    # SORTING NUMERICALLY

    def extract_file_index(filename):
        base = filename.replace(".csv", "")
        return int(base.split("_")[1])

    sorted_files = sorted(all_files, key=extract_file_index)
    print(f"[INFO] Sorted files: {sorted_files}")

    all_rows = []

    for filename in sorted_files:
        file_index = extract_file_index(filename)
        filepath   = os.path.join(partition_path, filename)

        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.append((file_index, row))

        print(f"[INFO] Read {filename}  (index={file_index})")

    # === TASK 4: RESOLVE LATEST RECORD PER ID ===

    latest_records = {}

    for file_index, row in all_rows:
        record_id = row["id"]

        if record_id not in latest_records:
            latest_records[record_id] = (file_index, row)
        else:
            existing_index, _ = latest_records[record_id]
            if file_index > existing_index:
                latest_records[record_id] = (file_index, row)

    resolved = {id: row_dict for id, (file_index, row_dict) in latest_records.items()}

    print(f"[INFO] Resolved {len(resolved)} unique IDs for this run:")
    for id, row in resolved.items():
        print(f"       id={id} → {row}")

    # === TASK 5: UPSERT INTO result.csv ===
    os.makedirs(OUTPUT_DIR, exist_ok=True)  

    if not os.path.isfile(OUTPUT_CSV):
        # FIRST RUN
        print("[INFO] result.csv not found — create new.")

        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(resolved.values())

        print(f"[INFO] result.csv create with {len(resolved)} rows.")

    else:
        # SUBSEQUENT RUNS
        print("[INFO] result.csv found — upsert.")

        # READ EXISTING ROWS
        existing = {}
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing[row["id"]] = row

        print(f"[INFO] In existing result.csv {len(existing)} rows.")

       
        for id, row in resolved.items():
            if id in existing:
                print(f"       UPDATE  id={id}")
            else:
                print(f"       INSERT  id={id}")
            existing[id] = row 

        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(existing.values())

        print(f"[INFO] result.csv updated — total {len(existing)} rows.")


# === TASK 1: DAG DEFINITION ===

with DAG(
    dag_id = "csv_upsert_pipeline",
    start_date = datetime(2026, 5, 1, 0, 0, 0),
    end_date = datetime(2026, 5, 6, 23),
    schedule = "@hourly",
    catchup = True,    
    tags = ["csv", "upsert", "incremental"],
) as dag:

    process_task = PythonOperator(
        task_id         = "process_partition",
        python_callable = process_partition,
    )
    
    process_task
