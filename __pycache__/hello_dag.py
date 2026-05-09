from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello from Airflow!")

with DAG("hello_dag", start_date=datetime(2024,1,1), schedule="@daily") as dag:
    task = PythonOperator(task_id="hello_task", python_callable=say_hello)
