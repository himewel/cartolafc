import json
import logging

import fsspec
import pandas as pd

from . import AbstractTransformer


class Transformer2021(AbstractTransformer):
    def get_scouts(self, next_execution_date):
        scouts_df = pd.DataFrame()
        str_search = f"{self.remote_path}/atletas_pontuados/*.json"
        logging.info(f"Searching for files in {str_search}...")

        with fsspec.open_files(str_search) as file_list:
            logging.info(f"{len(file_list)} files were found")
            for file in file_list:
                tmp_json = json.loads(file.read())
                tmp_df = pd.DataFrame.from_dict(
                    tmp_json["atletas"],
                    dtype="object",
                    orient="index",
                ).reset_index()
                tmp_df["rodada"] = tmp_json["rodada"]
                logging.info(f"Concatenating file from round {tmp_json['rodada']}")
                scouts_df = pd.concat(
                    objs=[scouts_df, tmp_df],
                    ignore_index=True,
                    sort=False,
                )

        scouts_df = scouts_df[scouts_df.entrou_em_campo == True]
        scouts_df.reset_index(drop=True, inplace=True)

        logging.info(f"Flattening scout values...")
        flat_scouts = scouts_df["scout"].apply(lambda x: {} if pd.isna(x) else x)
        flat_scouts = pd.json_normalize(flat_scouts)
        scouts_df.drop("scout", axis=1, inplace=True)
        scouts_df = pd.concat([scouts_df, flat_scouts], axis=1, sort=False)

        scouts_df["pontosMedia"] = 0
        scouts_df["preco"] = 0
        scouts_df["precoVariacao"] = 0
        scouts_df["DD"] = 0

        scouts_df.rename(
            columns={
                "index": "atletaID",
                "clube_id": "clubeID",
                "posicao_id": "posicaoID",
                "pontuacao": "pontos",
                "PI": "PE",
                "DS": "RB",
            },
            inplace=True,
        )

        partidas_df = self.get_partidas(next_execution_date)
        columns_to_get = ["partidaID", "rodada", "temporada"]
        mandantes = partidas_df[columns_to_get + ["clubeMandanteID"]].rename(
            columns={"clubeMandanteID": "clubeID"}
        )
        visitantes = partidas_df[columns_to_get + ["clubeVisitanteID"]].rename(
            columns={"clubeVisitanteID": "clubeID"}
        )
        unique_ids = pd.concat([mandantes, visitantes])

        unique_ids["rodada"] = unique_ids["rodada"].astype(int)
        unique_ids["clubeID"] = unique_ids["clubeID"].astype(int)
        scouts_df["rodada"] = scouts_df["rodada"].astype(int)
        scouts_df["clubeID"] = scouts_df["clubeID"].astype(int)

        scouts_df = scouts_df.merge(right=unique_ids, on=["rodada", "clubeID"])
        logging.info(f"Resulting dataframe: \n{scouts_df.head()}")
        logging.info(f"Shape: {scouts_df.shape}")
        return scouts_df

    def get_partidas(self, next_execution_date):
        partidas_df = pd.DataFrame()
        str_search = f"{self.remote_path}/partidas/*.json"
        logging.info(f"Searching for files in {str_search}...")

        with fsspec.open_files(str_search) as file_list:
            logging.info(f"{len(file_list)} files were found")
            for file in file_list:
                tmp_json = json.loads(file.read())
                tmp_df = pd.DataFrame.from_dict(tmp_json["partidas"], dtype=str)
                tmp_df["rodada"] = tmp_json["rodada"]
                logging.info(f"Concatenating file from round {tmp_json['rodada']}")
                partidas_df = pd.concat(
                    objs=[partidas_df, tmp_df],
                    ignore_index=True,
                    sort=False,
                )

        partidas_df["timestamp"] = pd.to_datetime(
            arg=partidas_df["timestamp"],
            utc=True,
            unit="s",
        ).dt.tz_convert("America/Sao_Paulo")
        partidas_df = partidas_df[partidas_df.timestamp < next_execution_date]
        partidas_df.reset_index(drop=True, inplace=True)

        partidas_df.rename(
            columns={
                "partida_id": "partidaID",
                "clube_casa_id": "clubeMandanteID",
                "clube_visitante_id": "clubeVisitanteID",
                "placar_oficial_mandante": "mandantePlacar",
                "placar_oficial_visitante": "visitantePlacar",
            },
            inplace=True,
        )

        with fsspec.open(f"{self.remote_path}/mercado_status/1.json") as stream:
            mercado_status = json.loads(stream.read())

        partidas_df["resultado"] = partidas_df.apply(self.get_resultado, axis=1)
        partidas_df["temporada"] = mercado_status["temporada"]
        partidas_df["mandantePlacar"] = partidas_df["mandantePlacar"].astype(float)
        partidas_df["visitantePlacar"] = partidas_df["visitantePlacar"].astype(float)

        logging.info(f"Resulting dataframe: \n{partidas_df.head()}")
        logging.info(f"Shape: {partidas_df.shape}")
        return partidas_df

    def get_atletas(self):
        str_search = f"{self.remote_path}/atletas_mercado/1.json"
        logging.info(f"Searching for files in {str_search}...")
        with fsspec.open(str_search) as file:
            tmp_json = json.loads(file.read())
            atletas_df = pd.DataFrame.from_dict(tmp_json["atletas"], dtype=str)

        str_search = f"{self.remote_path}/mercado_status/1.json"
        logging.info(f"Searching for files in {str_search}...")
        with fsspec.open(str_search) as file:
            mercado_status = json.loads(file.read())

        atletas_df["temporada"] = mercado_status["temporada"]
        atletas_df.rename(columns={"atleta_id": "atletaID"}, inplace=True)

        logging.info(f"Resulting dataframe: \n{atletas_df.head()}")
        logging.info(f"Shape: {atletas_df.shape}")
        return atletas_df

    def get_clubes(self):
        str_search = f"{self.remote_path}/clubes/1.json"
        logging.info(f"Searching for files in {str_search}...")

        with fsspec.open(str_search) as file:
            tmp_json = json.loads(file.read())
            clubes_df = pd.DataFrame.from_dict(
                tmp_json,
                dtype=str,
                orient="index",
            )

        clubes_df.rename(columns={"id": "clubeID"}, inplace=True)
        logging.info(f"Resulting dataframe: \n{clubes_df.head()}")
        logging.info(f"Shape: {clubes_df.shape}")
        return clubes_df

    def get_posicoes(self):
        str_search = f"{self.remote_path}/posicoes/1.json"
        logging.info(f"Searching for files in {str_search}...")

        with fsspec.open(str_search) as file:
            tmp_json = json.loads(file.read())
            posicoes_df = pd.DataFrame.from_dict(
                tmp_json,
                dtype="object",
                orient="index",
            )

        posicoes_df.rename(columns={"id": "posicaoID"}, inplace=True)
        logging.info(f"Resulting dataframe: \n{posicoes_df.head()}")
        logging.info(f"Shape: {posicoes_df.shape}")
        return posicoes_df
