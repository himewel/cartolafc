import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from include import Extractor

airflow_home = os.getenv("AIRFLOW_HOME")

with DAG(
    dag_id="cartolafc_extraction",
    schedule_interval="@yearly",
    start_date=datetime(2015, 1, 1),
) as dag:
    extractor = Extractor(
        base_url="https://api.github.com/repos/henriquepgomide/caRtola/contents/data",
        path=f"{airflow_home}/data",
    )

    PythonOperator(
        task_id="get_csv",
        python_callable=extractor.extract,
        op_kwargs={"year": "{{ execution_date.year }}"},
    )
