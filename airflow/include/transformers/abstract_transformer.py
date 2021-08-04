import os
import logging
from abc import ABC, abstractmethod

import pandas as pd
import yaml
from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook


class AbstractTransformer(ABC):
    def __init__(self, input_path, output_path="", schema_path="", execution_date=None):
        self.input_path = input_path
        self.output_path = output_path
        self.schema_path = schema_path
        self.execution_date = execution_date
        self.schema = {}
        self.dict_methods = {
            "scouts": self.get_scouts,
            "partidas": self.get_partidas,
            "atletas": self.get_atletas,
            "clubes": self.get_clubes,
            "posicoes": self.get_posicoes,
        }

        if schema_path:
            self.schema = self.get_schema()
        if execution_date:
            self.remote_path = self.get_remote_path()

    def get_schema(self):
        logging.info(f"Reading schema file from {self.schema_path}")
        with open(self.schema_path) as stream:
            schema = yaml.safe_load(stream)
        return schema

    def get_remote_path(self):
        hdfs = self.get_conn()
        date = self.execution_date.date()
        return f"{hdfs}/{self.input_path}/{date}"

    def get_resultado(self, row):
        if row["mandantePlacar"] > row["visitantePlacar"]:
            return "Casa"
        elif row["mandantePlacar"] < row["visitantePlacar"]:
            return "Visitante"
        else:
            return "Empate"

    def get_conn(self):
        conn_string = HDFSHook.get_connection("hdfs_default").get_uri()
        return conn_string

    def write_parquet(self, df, schema, partition_by):
        # select and cast columns
        for column, properties in self.schema[schema].items():
            if properties['type'] in ["int64", "float64"]:
                df[column].fillna(0, inplace=True)

            df[column] = df[column].astype(properties['type'])

            if properties['type'] in ["str"]:
                df[column].fillna("INDEFINIDO", inplace=True)

        df = df[list(self.schema[schema].keys())]
        df.columns = df.columns.str.lower()

        # defines output structure
        hdfs = self.get_conn()
        if partition_by is None:
            df.to_parquet(
                path=f"{hdfs}/{self.output_path}/{schema}/1.parquet",
                index=False,
            )
        else:
            df.to_parquet(
                path=f"{hdfs}/{self.output_path}/{schema}",
                partition_cols=partition_by,
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

    def get_clubes(self):
        clubes_df = pd.read_csv(f"{self.remote_path}/times_ids/1.csv")
        clubes_df = (
            clubes_df[["id", "nome.cbf", "abreviacao"]]
            .rename(columns={"id": "clubeID", "nome.cbf": "nome"})
            .drop_duplicates(subset=["clubeID"], keep="last")
        )
        bragantino = {
            "clubeID": 280,
            "nome": "Bragantino - SP",
            "abreviacao": "BGT",
        }
        clubes_df = clubes_df.append(bragantino, ignore_index=True)
        return clubes_df

    def get_posicoes(self):
        posicoes_df = pd.read_csv(f"{self.remote_path}/posicoes_ids/1.csv")
        posicoes_df.rename(
            columns={
                "Cod": "posicaoID",
                "Position": "nome",
                "abbr": "abreviacao",
            },
            inplace=True,
        )
        return posicoes_df
