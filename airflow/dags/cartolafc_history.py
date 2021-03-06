import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.task_group import TaskGroup

from datahub_provider.entities import Dataset

from include import YearlyFactory, datahub_update
from include.extractors import GithubExtractor

_RAW_PATH = "raw/history"
_CURATED_PATH = "curated"

_AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")
_INCLUDE_PATH = f"{_AIRFLOW_HOME}/include"
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
    description="Run automated data ingestion of CartolaFC to HDFS and Hive from 2014 to 2020 seasons",
    max_active_runs=1,
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    end_date=datetime(2020, 1, 1),
    tags=["Hive", "HDFS", "DataHub"],
) as dag:
    extractor = GithubExtractor(path=_RAW_PATH)
    transformer = YearlyFactory(
        input_path=_RAW_PATH,
        output_path=_CURATED_PATH,
        schema_path=_SCHEMA_PATH,
    )

    with TaskGroup(group_id="extract") as ext_tg:
        clean_url = extractor.base_url.replace("https://", "").replace("/", ".")
        extract_dynamic_task = PythonOperator(
            task_id="extract_dynamic",
            python_callable=extractor.extract,
            op_kwargs={"mode": "static"},
            inlets={"datasets": [Dataset("http", clean_url)]},
            outlets={"datasets": [Dataset("hdfs", _RAW_PATH.replace("/", "."))]},
        )

        extract_static_task = PythonOperator(
            task_id="extract_static",
            python_callable=extractor.extract,
            op_kwargs={"mode": "dynamic"},
            inlets={"datasets": [Dataset("http", clean_url)]},
            outlets={"datasets": [Dataset("hdfs", _RAW_PATH.replace("/", "."))]},
        )

        extraction_tasks_list = [extract_dynamic_task, extract_static_task]

    transform_groups = []
    for table_name, transform_method in transformer.dict_methods.items():
        with TaskGroup(group_id=f"transform_{table_name}") as transf_tg:
            transform_task = PythonOperator(
                task_id=f"transform_{table_name}",
                python_callable=transform_method,
                inlets={"datasets": [Dataset("hdfs", _RAW_PATH.replace("/", "."))]},
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
