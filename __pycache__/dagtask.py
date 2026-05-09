from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

cities = ["karachi", "lahore", "islamabad", "peshawar", "quetta"]

def process_city(city):
    print(f"Processing city: {city.upper()}")
    print(f"  → Fetching weather data for {city}...")
    print(f"  → Done with {city}!")

with DAG(
    dag_id="dynamic_city_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Starting dynamic DAG...")
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("All cities processed!")
    )

    city_tasks = []
    for city in cities:
        task = PythonOperator(
            task_id=f"process_{city}",
            python_callable=process_city,
            op_kwargs={"city": city}
        )
        city_tasks.append(task)

    start >> city_tasks >> end