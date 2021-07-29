import os
from datetime import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.task_group import TaskGroup

from datahub_provider.entities import Dataset

from include import GithubExtractor, TransformFactory, datahub_update

_AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")

_DATA_PATH = f"{_AIRFLOW_HOME}/data"
_INCLUDE_PATH = f"{_AIRFLOW_HOME}/include"

_RAW_PATH = f"{_DATA_PATH}/raw"
_CURATED_PATH = f"{_DATA_PATH}/curated"

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
        output_path=_CURATED_PATH,
        schema_path=_SCHEMA_PATH,
    )

    transform_methods = {
        "scouts": transformer.get_scouts,
        "partidas": transformer.get_partidas,
        "atletas": transformer.get_atletas,
        "clubes": transformer.get_clubes,
        "posicoes": transformer.get_posicoes,
    }

    with TaskGroup(group_id="extract") as ext_tg:
        clean_url = _API_URL.replace("https://", "").replace("/", ".")
        extract_dynamic_task = PythonOperator(
            task_id="extract_dynamic",
            python_callable=extractor.extract_dynamic_files,
            op_kwargs={"year": "{{ execution_date.year }}"},
            inlets={"datasets": [Dataset("http", clean_url)]},
            outlets={"datasets": [Dataset("hdfs", "raw")]},
        )

        extract_static_task = PythonOperator(
            task_id="extract_static",
            python_callable=extractor.extract_static_files,
            op_kwargs={"year": "{{ execution_date.year }}"},
            inlets={"datasets": [Dataset("http", clean_url)]},
            outlets={"datasets": [Dataset("hdfs", "raw")]},
        )

        extraction_tasks_list = [extract_dynamic_task, extract_static_task]

    transform_groups = []
    for table_name, transform_method in transform_methods.items():
        with TaskGroup(group_id=f"transform_{table_name}") as transf_tg:
            transform_task = PythonOperator(
                task_id=f"transform_{table_name}",
                python_callable=transform_method,
                op_kwargs={"year": "{{ execution_date.year }}"},
                inlets={"datasets": [Dataset("hdfs", "raw")]},
                outlets={
                    "datasets": [
                        Dataset("hdfs", f"curated.{table_name}"),
                        Dataset("hive", f"curated.{table_name}"),
                    ]
                },
            )

            if table_name == "atletas":
                update_table_task = HiveOperator(
                    task_id=f"upsert_hive_{table_name}",
                    hql=_UPSERT_ATLETAS.read(),
                    inlets={
                        "datasets": [
                            Dataset("hdfs", f"curated.{table_name}"),
                            Dataset("hive", f"curated.{table_name}"),
                        ]
                    },
                    outlets={"datasets": [Dataset("hive", f"trusted.{table_name}")]},
                )
            else:
                update_table_task = HiveOperator(
                    task_id=f"update_hive_{table_name}",
                    hql=f"""
                        MSCK REPAIR TABLE curated.{table_name};
                        TRUNCATE TABLE trusted.{table_name};
                        INSERT INTO trusted.{table_name}
                        SELECT * FROM curated.{table_name};
                        MSCK REPAIR TABLE trusted.{table_name};
                    """,
                    inlets={
                        "datasets": [
                            Dataset("hdfs", f"curated.{table_name}"),
                            Dataset("hive", f"curated.{table_name}"),
                        ]
                    },
                    outlets={"datasets": [Dataset("hive", f"trusted.{table_name}")]},
                )

            transform_task >> update_table_task

        transform_groups.append(transf_tg)
        ext_tg >> transf_tg

    datahub_update = PythonOperator(
        task_id="update_datahub_schema",
        python_callable=datahub_update,
    )

    transform_groups >> datahub_update
