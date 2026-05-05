from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator 
from datetime import datetime, timezone

def print_interval(**context):

    start = context["data_interval_start"]
    end   = context["data_interval_end"]

    print("=== INTERVAL INFO ===")
    print(f"Interval Start:  {start}")
    print(f"Interval End:    {end}")
    print(f"Duration:        {end - start}")

    print("=== ALL AVAILABLE MACROS ===")
    for key, value in context.items():
        print(f"{key}: {value}")

with DAG(
    dag_id="print_interval_dag",
    start_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
    schedule="0 */3 * * *",
    catchup=True,
) as dag:

    print_task = PythonOperator(
        task_id="print_interval",
        python_callable=print_interval,
    )