from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

cities = ["karachi", "lahore", "islamabad", "peshawar", "quetta"]

def process_city(city):
    print(f"Processing city: {city.upper()}")
    print(f"  → Fetching weather data for {city}...")
    print(f"  → Done with {city}!")

# loop runs at the FILE level — outside any DAG
for city in cities:

    with DAG(
        dag_id=f"dag_{city}",        # creates dag_karachi, dag_lahore etc.
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False
    ) as dag:

        PythonOperator(
            task_id=f"process_{city}",
            python_callable=process_city,
            op_kwargs={"city": city}
        )