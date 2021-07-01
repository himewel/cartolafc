import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from include import RawExtractor, TransformFactory

_AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")
_DATA_PATH = f"{_AIRFLOW_HOME}/data"
_RAW_PATH = f"{_DATA_PATH}/raw"
_SCHEMA_PATH = f"{_AIRFLOW_HOME}/include/schema.yaml"
_API_URL = "https://api.github.com/repos/henriquepgomide/caRtola/contents/data"
_TABLES = ["scouts", "partidas", "atletas", "clubes", "posicoes"]

with DAG(
    dag_id="cartolafc_history",
    schedule_interval="@yearly",
    start_date=datetime(2015, 1, 1),
    max_active_runs=1,
) as dag:
    extractor = RawExtractor(base_url=_API_URL, path=_DATA_PATH)
    transformer = TransformFactory(path=_RAW_PATH)

    table_schema = {
        "scouts": transformer.get_scouts,
        "partidas": transformer.get_partidas,
        "atletas": transformer.get_atletas,
        "clubes": transformer.get_clubes,
        "posicoes": transformer.get_posicoes,
    }

    extract_dynamic_task = PythonOperator(
        task_id="extract_dynamic",
        python_callable=extractor.extract_dynamic_files,
        op_kwargs={"year": "{{ execution_date.year }}"},
    )

    extract_static_task = PythonOperator(
        task_id="extract_static",
        python_callable=extractor.extract_static_files,
        op_kwargs={"year": "{{ execution_date.year }}"},
    )

    extraction_tasks = [extract_dynamic_task, extract_static_task]

    for table_name, transform_method in table_schema.items():
        transform_task = PythonOperator(
            task_id=f"transform_{table_name}",
            python_callable=transform_method,
            op_kwargs={"year": "{{ execution_date.year }}"},
        )

        extraction_tasks >> transform_task
