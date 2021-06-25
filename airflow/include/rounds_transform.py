import logging
from glob import glob

import pandas as pd
import yaml


class RoundsTransform:
    def __init__(self, path, schema_path):
        self.path = path
        with open(schema_path) as stream:
            self.schema = yaml.safe_load(stream)

    def get_concated_rounds(self, year):
        folder_name = f"{self.path}/{year}"
        round_files = glob(f"{folder_name}/rodada/*.csv")

        scouts = pd.DataFrame()
        for round in round_files:
            tmp_df = pd.read_csv(round)
            scouts = pd.concat([scouts, tmp_df], sort=True)
        return scouts

    def get_atletas(self, year):
        current_schema = self.schema[year]
        atletas_schema = current_schema["atletas"]
        folder_name = f"{self.path}/{year}"

        if year < "2019":
            atletas = pd.read_csv(f"{folder_name}/jogadores/1.csv")
        else:
            atletas = self.get_concated_rounds(year)

        atletas = atletas.rename(columns=atletas_schema)[list(atletas_schema.values())]
        atletas["referencia"] = year

        logging.debug(f"\n{atletas.head(50)}")
        logging.debug(f"\n{atletas.dtypes}")

    def get_scouts(self, year):
        current_schema = self.schema[year]
        scouts_schema = current_schema["scouts"]

        scouts = self.get_concated_rounds(year)
        scouts = scouts.rename(columns=scouts_schema)[list(scouts_schema.values())]
        scouts["referencia"] = year

        logging.debug(f"\n{scouts.head(50)}")
        logging.debug(f"\n{scouts.dtypes}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    scouts = RoundsTransform(
        path="./data/raw",
        schema_path="./include/schema.yaml",
    )
    scouts.get_atletas("2020")
    scouts.get_scouts("2020")
