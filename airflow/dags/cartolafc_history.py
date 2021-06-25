import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from include import RawExtractor, ScoutsTransform, RoundsTransform

_AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")
_DATA_PATH = f"{_AIRFLOW_HOME}/data"
_RAW_PATH = f"{_DATA_PATH}/raw"
_SCHEMA_PATH = f"{_AIRFLOW_HOME}/include/schema.yaml"
_API_URL = "https://api.github.com/repos/henriquepgomide/caRtola/contents/data"

branches = {
    "scouts_branch": ScoutsTransform(path=_RAW_PATH, schema_path=_SCHEMA_PATH),
    "rounds_branch": RoundsTransform(path=_RAW_PATH, schema_path=_SCHEMA_PATH),
}


def branch_decision(year):
    branch_list = list(branches.keys())
    if int(year) < 2018:
        return branch_list[0]
    else:
        return branch_list[1]


with DAG(
    dag_id="cartolafc_history",
    schedule_interval="@yearly",
    start_date=datetime(2015, 1, 1),
    max_active_runs=1,
) as dag:
    extractor = RawExtractor(base_url=_API_URL, path=_DATA_PATH)

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

    branch_task = BranchPythonOperator(
        task_id="branching",
        python_callable=branch_decision,
        op_kwargs={"year": "{{ execution_date.year }}"},
    )

    [extract_dynamic_task, extract_static_task] >> branch_task

    for branch_name, transform_class in branches.items():
        dummy_branch = DummyOperator(task_id=branch_name)

        transform_list = []
        table_schema = {
            "atletas": transform_class.get_atletas,
            "scouts": transform_class.get_scouts,
        }
        for table_name, transform_method in table_schema.items():
            transform_task = PythonOperator(
                task_id=f"{branch_name}_{table_name}_table",
                python_callable=transform_method,
                op_kwargs={"year": "{{ execution_date.year }}"},
            )
            transform_list.append(transform_task)

        branch_task >> dummy_branch >> transform_list
