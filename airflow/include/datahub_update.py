import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.run.pipeline import Pipeline


def datahub_update(**kwargs):
    from datahub.integrations.airflow.hooks import DatahubGenericHook

    hive_conn = DatahubGenericHook.get_connection("hive_cli_default")
    datahub_conn = DatahubGenericHook.get_connection("datahub_rest_default")

    create_hive_recipe(hive_conn, datahub_conn)
    add_superset_lineage(datahub_conn)
    fix_superset_urn(datahub_conn)


def create_hive_recipe(hive_conn, datahub_conn):
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


def add_superset_lineage(datahub_conn):
    emitter = DatahubRestEmitter(datahub_conn.host)

    lineage_mce = builder.make_lineage_mce(
        [
            builder.make_dataset_urn("hive", "refined.clubes"),
            builder.make_dataset_urn("hive", "refined.partidas"),
        ],
        builder.make_dataset_urn("hive", "superset.partidas_full"),
        "VIEW",
    )
    emitter.emit_mce(lineage_mce)

    lineage_mce = builder.make_lineage_mce(
        [
            builder.make_dataset_urn("hive", "refined.clubes"),
            builder.make_dataset_urn("hive", "refined.partidas"),
            builder.make_dataset_urn("hive", "refined.atletas"),
            builder.make_dataset_urn("hive", "refined.scouts"),
        ],
        builder.make_dataset_urn("hive", "superset.scouts_full"),
        "VIEW",
    )
    emitter.emit_mce(lineage_mce)


def fix_superset_urn(datahub_conn):
    emitter = DatahubRestEmitter(datahub_conn.host)

    lineage_mce = builder.make_lineage_mce(
        [builder.make_dataset_urn("hive", "superset.partidas_full")],
        builder.make_dataset_urn("hive", "hive.superset.partidas_full"),
        "COPY",
    )
    emitter.emit_mce(lineage_mce)

    lineage_mce = builder.make_lineage_mce(
        [builder.make_dataset_urn("hive", "superset.scouts_full")],
        builder.make_dataset_urn("hive", "hive.superset.scouts_full"),
        "COPY",
    )
    emitter.emit_mce(lineage_mce)


def hive_update():
    recipe = {
        "source": {
            "type": "hive",
            "config": {"host_port": "hive:10000"},
        },
        "sink": {
            "type": "datahub-rest",
            "config": {"server": "http://datahub-gms:8080"},
        },
    }

    pipeline = Pipeline.create(recipe)
    pipeline.run()


if __name__ == "__main__":
    hive_update()
