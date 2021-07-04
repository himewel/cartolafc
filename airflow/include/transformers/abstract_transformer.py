import os
from abc import ABC, abstractmethod

import yaml


class AbstractTransformer(ABC):
    def __init__(self, input_path, output_path="", schema_path=""):
        self.input_path = input_path
        self.output_path = output_path
        self.schema = {}

        if schema_path:
            with open(schema_path) as stream:
                self.schema = yaml.safe_load(stream)

    def write_parquet(self, df, schema, partition_by):
        partition_by = partition_by or "1"
        for column, properties in self.schema[schema].items():
            if properties['type'] in ["int64", "float64"]:
                df[column].fillna(0, inplace=True)

            df[column] = df[column].astype(properties['type'])

            if properties['type'] in ["str"]:
                df[column].fillna("INDEFINIDO", inplace=True)

        df = df[list(self.schema[schema].keys())]

        os.makedirs(f"{self.output_path}/{schema}", exist_ok=True)
        df.to_parquet(
            path=f"{self.output_path}/{schema}/{partition_by}.parquet",
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

    def get_clubes(self, year):
        clubes_df = pd.read_csv(f"{self.input_path}/{year}/times_ids/1.csv")
        clubes_df = (
            clubes_df[["id", "nome.cbf", "abreviacao"]]
            .rename(columns={"id": "clubeID", "nome.cbf": "nome"})
            .drop_duplicates(subset=["clubeID"], keep="last")
        )
        return clubes_df

    def get_posicoes(self, year):
        posicoes_df = pd.read_csv(f"{self.input_path}/{year}/posicoes_ids/1.csv")
        posicoes_df.rename(
            columns={
                "Cod": "posicaoID",
                "Position": "nome",
                "abbr": "abreviacao",
            },
            inplace=True,
        )
        return posicoes_df
