import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.task_group import TaskGroup

from datahub_provider.entities import Dataset

from include import GithubExtractor, TransformFactory, datahub_update

_AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")

_DATA_PATH = f"{_AIRFLOW_HOME}/data"
_INCLUDE_PATH = f"{_AIRFLOW_HOME}/include"

_RAW_PATH = f"{_DATA_PATH}/raw"
_TRUSTED_PATH = f"{_DATA_PATH}/trusted"

_API_URL = "https://api.github.com/repos/henriquepgomide/caRtola/contents/data"

_SCHEMA_PATH = f"{_INCLUDE_PATH}/schema.yaml"
_UPSERT_ATLETAS = open(f"{_INCLUDE_PATH}/hql/upsert_atletas.hql")

default_args = {
    "depends_on_past": True,
    "retries": 5,
    "wait_for_downstream": True,
}

with DAG(
    dag_id="cartolafc_history",
    default_args=default_args,
    description="Run automated data ingestion from CartolaFC to HDFS and Hive",
    max_active_runs=1,
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=["Hive", "HDFS", "DataHub"],
) as dag:
    extractor = GithubExtractor(base_url=_API_URL, path=_RAW_PATH)
    transformer = TransformFactory(
        input_path=_RAW_PATH,
        output_path=_TRUSTED_PATH,
        schema_path=_SCHEMA_PATH,
    )

    transform_methods = {
        "scouts": transformer.get_scouts,
        "partidas": transformer.get_partidas,
        "atletas": transformer.get_atletas,
        "clubes": transformer.get_clubes,
        "posicoes": transformer.get_posicoes,
    }

    with TaskGroup(group_id="environment_setup") as env_tg:
        create_folders_task = BashOperator(
            task_id="create_hdfs_path",
            bash_command="hdfs dfs -mkdir -p /raw /trusted",
        )

    with TaskGroup(group_id="extract") as ext_tg:
        extract_dynamic_task = PythonOperator(
            task_id="extract_dynamic",
            python_callable=extractor.extract_dynamic_files,
            op_kwargs={"year": "{{ execution_date.year }}"},
            inlets={
                "datasets": [
                    Dataset("http", _API_URL.replace("https://", "").replace("/", "."))
                ]
            },
        )

        extract_static_task = PythonOperator(
            task_id="extract_static",
            python_callable=extractor.extract_static_files,
            op_kwargs={"year": "{{ execution_date.year }}"},
            inlets={
                "datasets": [
                    Dataset("http", _API_URL.replace("https://", "").replace("/", "."))
                ]
            },
        )

        extraction_tasks_list = [extract_dynamic_task, extract_static_task]

        raw_upload_task = BashOperator(
            task_id="raw_upload",
            bash_command="hdfs dfs -copyFromLocal -f {raw_path}/{year} /raw".format(
                raw_path=_RAW_PATH,
                year="{{ execution_date.year }}",
            ),
            outlets={"datasets": [Dataset("hdfs", "raw.{{ execution_date.year }}")]},
        )

        extraction_tasks_list >> raw_upload_task

    env_tg >> ext_tg

    transform_groups = []
    for table_name, transform_method in transform_methods.items():
        with TaskGroup(group_id=f"transform_{table_name}") as transf_tg:
            transform_task = PythonOperator(
                task_id=f"transform_{table_name}",
                python_callable=transform_method,
                op_kwargs={"year": "{{ execution_date.year }}"},
                inlets={"datasets": [Dataset("hdfs", "raw.{{ execution_date.year }}")]},
            )

            trusted_upload_task = BashOperator(
                task_id=f"upload_{table_name}",
                bash_command=f"""
                    hdfs dfs -copyFromLocal -f \
                        {_TRUSTED_PATH}/{table_name} /trusted
                """,
                outlets={
                    "datasets": [
                        Dataset("hdfs", f"trusted.{table_name}"),
                        Dataset("hive", f"trusted.{table_name}"),
                    ]
                },
            )

            if table_name == "atletas":
                update_table_task = HiveOperator(
                    task_id=f"upsert_hive_{table_name}",
                    hql=_UPSERT_ATLETAS.read(),
                    inlets={"datasets": [Dataset("hive", f"trusted.{table_name}")]},
                    outlets={"datasets": [Dataset("hive", f"refined.{table_name}")]},
                )
            else:
                update_table_task = HiveOperator(
                    task_id=f"update_hive_{table_name}",
                    hql=f"""
                        MSCK REPAIR TABLE trusted.{table_name};
                        TRUNCATE TABLE refined.{table_name};
                        INSERT INTO refined.{table_name}
                        SELECT * FROM trusted.{table_name};
                        MSCK REPAIR TABLE refined.{table_name};
                    """,
                    inlets={"datasets": [Dataset("hive", f"trusted.{table_name}")]},
                    outlets={"datasets": [Dataset("hive", f"refined.{table_name}")]},
                )

            transform_task >> trusted_upload_task >> update_table_task

        transform_groups.append(transf_tg)
        ext_tg >> transf_tg

    datahub_update = PythonOperator(
        task_id="update_datahub_schema",
        python_callable=datahub_update,
    )
    transform_groups >> datahub_update
