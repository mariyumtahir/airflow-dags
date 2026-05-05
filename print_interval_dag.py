from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator 
from datetime import datetime

def print_interval(**context):
    print("=== ALL AVAILABLE MACROS ===")
    for key, value in context.items():
        print(f"{key}: {value}")

with DAG(
    dag_id="print_interval_dag",
    start_date=datetime(2025, 5, 1),
    schedule="0 */3 * * *",
    catchup=True,
) as dag:

    print_task = PythonOperator(
        task_id="print_interval",
        python_callable=print_interval,
    )