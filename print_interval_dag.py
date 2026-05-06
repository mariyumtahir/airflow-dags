from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from croniter import croniter
from datetime import datetime, timezone

def print_interval(**context):
    dag_run = context["dag_run"]
    dag = context["dag"]

    start = dag_run.data_interval_start
    schedule = dag.schedule  # e.g. "0 */3 * * *"

    # Get the next scheduled time after start = our interval end
    cron = croniter(schedule, start)
    end = cron.get_next(datetime)

    print("=== INTERVAL INFO ===")
    print(f"Interval Start:  {start}")
    print(f"Interval End:    {end}")
    print(f"Duration:        {end - start}")

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