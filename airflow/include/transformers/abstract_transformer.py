from abc import ABC, abstractmethod

import yaml


class AbstractTransformer(ABC):
    def __init__(self, path, schema_path=""):
        self.path = path
        self.schema = {}

        if schema_path:
            with open(schema_path) as stream:
                self.schema = yaml.safe_load(stream)

    def write_parquet(self, df, schema, partition_by):
        df = df[list(self.schema[schema].keys())]
        for column, dtype in self.schema[schema].items():
            df[column] = df[column].astype(dtype)

        df.to_parquet(
            path=f"{self.path}/{schema}/{year}.parquet",
            index=False,
        )

    @abstractmethod
    def get_scouts(self, year):
        pass

    @abstractmethod
    def get_partidas(self, year):
        pass

    @abstractmethod
    def get_atletas(self, year):
        pass

    @abstractmethod
    def get_clubes(self, year):
        pass

    @abstractmethod
    def get_posicoes(self, year):
        pass
