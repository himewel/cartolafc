import logging

import pandas as pd
import yaml


class ScoutsTransform:
    def __init__(self, path, schema_path):
        self.path = path
        with open(schema_path) as stream:
            self.schema = yaml.safe_load(stream)

    def custom_clubeid(self, df):
        df["ClubeID"] = df["ClubeID_x"].combine_first(df["ClubeID_y"])
        return df

    def join_scout_players(self, year):
        current_schema = self.schema[year]
        keys = current_schema["keys"]

        folder_name = f"{self.path}/{year}"

        scouts_df = pd.read_csv(f"{folder_name}/scouts_raw/1.csv")
        jogadores_df = pd.read_csv(f"{folder_name}/jogadores/1.csv")

        joined_df = scouts_df.merge(
            right=jogadores_df,
            left_on=[keys["scouts"]],
            right_on=[keys["atletas"]],
        )

        logging.debug(joined_df.head(50))
        logging.debug(joined_df.dtypes)
        return joined_df

    def get_atletas(self, year):
        current_schema = self.schema[year]
        atletas_schema = current_schema["atletas"]

        atletas = self.join_scout_players(year).copy()

        if year in ["2015", "2016"]:
            atletas = self.custom_clubeid(atletas)

        atletas = atletas.rename(columns=atletas_schema)[list(atletas_schema.values())]
        atletas["referencia"] = year

        logging.debug(atletas.head(50))
        logging.debug(atletas.dtypes)

    def get_scouts(self, year):
        current_schema = self.schema[year]
        scouts_schema = current_schema["scouts"]

        scouts = self.join_scout_players(year).copy()

        if year in ["2015", "2016"]:
            atletas = self.custom_clubeid(scouts)

        scouts = scouts.rename(columns=scouts_schema)[list(scouts_schema.values())]
        scouts["referencia"] = year

        logging.debug(scouts.head(50))
        logging.debug(scouts.dtypes)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    scouts = ScoutTransform(
        path="./data/raw",
        schema_path="./include/schema.yaml",
    )
    scouts.get_scouts("2017")
    scouts.get_atletas("2017")
