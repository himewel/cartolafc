from datahub.ingestion.run.pipeline import Pipeline
from datahub.integrations.airflow.hooks import DatahubGenericHook


def datahub_update(**kwargs):
    hive_conn = DatahubGenericHook.get_connection("hive_cli_default")
    datahub_conn = DatahubGenericHook.get_connection("datahub_rest_default")

    recipe = {
        "source": {
            "type": "hive",
            "config": {"host_port": hive_conn.host},
        },
        "sink": {
            "type": "datahub-rest",
            "config": {"server": datahub_conn.host},
        },
    }

    pipeline = Pipeline.create(recipe)
    pipeline.run()
    pipeline.raise_from_status()
