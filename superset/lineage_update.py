from datahub.ingestion.run.pipeline import Pipeline


def superset_update():
    recipe = {
        "source": {
            "type": "superset",
            "config": {
                "username": "admin",
                "password": "admin",
                "provider": "db",
                "connect_uri": "http://superset:8088",
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": {"server": "http://datahub-gms:8080"},
        },
    }

    pipeline = Pipeline.create(recipe)
    pipeline.run()


if __name__ == "__main__":
    superset_update()
