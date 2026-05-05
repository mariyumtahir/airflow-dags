from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_interval(**context):
    start = context["data_interval_start"]
    end   = context["data_interval_end"]
    print(f"Interval Start: {start}")
    print(f"Interval End:   {end}")

with DAG(
    dag_id="print_interval_dag",
    start_date=datetime(2025, 5, 1),
    schedule="0 */3 * * *",
    catchup=True,
    tags=["example"],
) as dag:

    print_task = PythonOperator(
        task_id="print_interval",
        python_callable=print_interval,
    )