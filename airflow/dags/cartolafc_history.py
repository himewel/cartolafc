import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from include import RawExtractor, TransformFactory

_AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")

_DATA_PATH = f"{_AIRFLOW_HOME}/data"
_RAW_PATH = f"{_DATA_PATH}/raw"
_TRUSTED_PATH = f"{_DATA_PATH}/trusted"
_SCHEMA_PATH = f"{_AIRFLOW_HOME}/include/schema.yaml"
_SCRIPTS_PATH = f"{_AIRFLOW_HOME}/include/scripts"

_API_URL = "https://api.github.com/repos/henriquepgomide/caRtola/contents/data"
_TABLES = ["scouts", "partidas", "atletas", "clubes", "posicoes"]

default_args = {
    "depends_on_past": True,
}

with DAG(
    dag_id="cartolafc_history",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    default_args=default_args,
    max_active_runs=1,
) as dag:
    extractor = RawExtractor(base_url=_API_URL, path=_RAW_PATH)
    transformer = TransformFactory(
        input_path=_RAW_PATH,
        output_path=_TRUSTED_PATH,
        schema_path=_SCHEMA_PATH,
    )

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

    extraction_tasks_list = [extract_dynamic_task, extract_static_task]

    raw_upload_task = BashOperator(
        task_id="raw_upload",
        bash_command="bash {scripts_path}/hdfs_upload.sh {raw_path}/{year} /raw".format(
            scripts_path=_SCRIPTS_PATH,
            raw_path=_RAW_PATH,
            year="{{ execution_date.year }}",
        ),
    )

    extraction_tasks_list >> raw_upload_task

    transform_tasks_list = []
    for table_name, transform_method in table_schema.items():
        transform_task = PythonOperator(
            task_id=f"transform_{table_name}",
            python_callable=transform_method,
            op_kwargs={"year": "{{ execution_date.year }}"},
        )
        transform_tasks_list.append(transform_task)

    raw_upload_task >> transform_tasks_list

    trusted_upload_task = BashOperator(
        task_id="trusted_upload",
        bash_command="bash {scripts_path}/hdfs_upload.sh {trusted_path} /trusted".format(
            scripts_path=_SCRIPTS_PATH,
            trusted_path=_TRUSTED_PATH,
        ),
    )

    transform_tasks_list >> trusted_upload_task
