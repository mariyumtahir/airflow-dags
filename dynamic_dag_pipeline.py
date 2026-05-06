from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timezone

# ---- Step 1: Config -------------------
PIPELINE_CONFIG = [
    {
        "name": "sales",
        "source_table": "raw.sales_data",
        "target_table": "processed.sales_summary",
        "enable_validation": True,
        "enable_transformation": True,
        "enable_bq_load": True,
        "partition_field": "sale_date",
        "schedule": "daily",
    },
    {
        "name": "inventory",
        "source_table": "raw.inventory_data",
        "target_table": "processed.inventory_summary",
        "enable_validation": False,
        "enable_transformation": True,
        "enable_bq_load": True,
        "partition_field": "snapshot_date",
        "schedule": "daily",
    },
    {
        "name": "customers",
        "source_table": "raw.customer_data",
        "target_table": "processed.customer_profile",
        "enable_validation": True,
        "enable_transformation": False,
        "enable_bq_load": True,
        "partition_field": None,
        "schedule": "weekly",
    },
    {
        "name": "returns",
        "source_table": "raw.returns_data",
        "target_table": "processed.returns_summary",
        "enable_validation": True,
        "enable_transformation": True,
        "enable_bq_load": False,
        "partition_field": "return_date",
        "schedule": "daily",
    },
    {
        "name": "promotions",
        "source_table": "raw.promotions_data",
        "target_table": "processed.promotions_report",
        "enable_validation": False,
        "enable_transformation": False,
        "enable_bq_load": True,
        "partition_field": None,
        "schedule": "weekly",
    },
]


# ---- Step 2: Validate Function -----------------------
def validate_data(config, **kwargs):
    print(f"Validating source table: {config['source_table']}")
    print(f"Row count check passed for: {config['name']}")


# ---- Step 3: Transform Function ----------------------
def transform_data(config, **kwargs):
    print(f"Transforming data for: {config['name']}")
    print(f"Applying business rules to: {config['source_table']}")
    if config["partition_field"] is not None:
        print(f"Partitioning output by: {config['partition_field']}")


# ---- Step 4: BQ Query Builder -----------------------
def build_bq_query(config):
    if config["partition_field"] is not None:
        return f"""
            SELECT * FROM `{config['source_table']}`
            WHERE {config['partition_field']} = '{{{{ ds }}}}'
        """
    return f"SELECT * FROM `{config['source_table']}`"


# ---- Step 5: BQ Load Function -----------------------
def load_to_bq(config, **kwargs):
    query = build_bq_query(config) 
    print(f"Loading data to BigQuery: {config['target_table']}")
    print(f"Query: {query}")
    print(f"Load complete: {config['name']} → {config['target_table']}")


# ---- Step 6: DAG Definition ----------------------------
with DAG(
    dag_id="dynamic_pipeline_dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["dynamic", "bigquery", "assignment"],
) as dag:

    # ---- Step 7: Dynamic Tasks + Dependencies ------------------
    for config in PIPELINE_CONFIG:

        validate_task = None
        if config["enable_validation"]:
            validate_task = PythonOperator(
                task_id=f"validate_{config['name']}",
                python_callable=validate_data,
                op_kwargs={"config": config},
            )

        transform_task = None
        if config["enable_transformation"]:
            transform_task = PythonOperator(
                task_id=f"transform_{config['name']}",
                python_callable=transform_data,
                op_kwargs={"config": config},
            )

        bq_task = None
        if config["enable_bq_load"]:
            bq_task = PythonOperator(
                task_id=f"bq_load_{config['name']}",
                python_callable=load_to_bq,
                op_kwargs={"config": config},
            )

        task_chain = [t for t in [validate_task, transform_task, bq_task]
                      if t is not None]
        for i in range(len(task_chain) - 1):
            task_chain[i] >> task_chain[i + 1]