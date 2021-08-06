import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from datahub_provider.entities import Dataset

from include import DailyFactory
from include.extractors import CartolaExtractor

_RAW_PATH = "raw/daily"
_CURATED_PATH = "curated"

_AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")
_INCLUDE_PATH = f"{_AIRFLOW_HOME}/include"
_SCHEMA_PATH = f"{_INCLUDE_PATH}/schema.yaml"

default_args = {
    "depends_on_past": True,
    "retries": 5,
    "wait_for_downstream": True,
}

with DAG(
    dag_id="cartolafc_daily",
    default_args=default_args,
    description="Run daily ingestion of CartolaFC to HDFS and Hive from REST API",
    max_active_runs=1,
    schedule_interval="@daily",
    start_date=days_ago(1),
    tags=["Hive", "HDFS", "DataHub"],
) as dag:
    extractor = CartolaExtractor(path=_RAW_PATH)
    transformer = DailyFactory(
        input_path=_RAW_PATH,
        output_path=_CURATED_PATH,
        schema_path=_SCHEMA_PATH,
    )

    clean_url = extractor.base_url.replace("https://", "").replace("/", ".")
    extraction_task = PythonOperator(
        task_id="api_extraction",
        python_callable=extractor.extract,
        inlets={"datasets": [Dataset("http", clean_url)]},
        outlets={"datasets": [Dataset("hdfs", _RAW_PATH.replace("/", "."))]},
    )

    for table_name, transform_method in transformer.dict_methods.items():
        curated_table = f"curated.{table_name}"
        transform_task = PythonOperator(
            task_id=f"transform_{table_name}",
            python_callable=transform_method,
            inlets={"datasets": [Dataset("hdfs", _RAW_PATH.replace("/", "."))]},
            outlets={
                "datasets": [
                    Dataset("hdfs", curated_table),
                    Dataset("hive", curated_table),
                ]
            },
        )

        extraction_task >> transform_task
