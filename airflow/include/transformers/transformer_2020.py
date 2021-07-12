from glob import glob

import pandas as pd

from . import AbstractTransformer


class Transformer2020(AbstractTransformer):
    def get_scouts(self):
        file_list = glob(f"{self.input_path}/2020/rodada/*.csv")
        scouts_df = pd.DataFrame()
        for file in file_list:
            tmp_df = pd.read_csv(file, dtype=str).drop_duplicates(
                subset=["atletas.clube_id", "atletas.atleta_id", "atletas.rodada_id"],
                keep="first",
            )
            scouts_df = pd.concat([scouts_df, tmp_df], sort=True)

        null_status = ["Nulo", "Contundido", "Suspenso"]
        scouts_df = scouts_df[~scouts_df["atletas.status_id"].isin(null_status)]

        scouts_df.rename(
            columns={
                "atletas.clube_id": "clubeID",
                "atletas.atleta_id": "atletaID",
                "atletas.rodada_id": "rodada",
                "atletas.pontos_num": "pontos",
                "atletas.media_num": "pontosMedia",
                "atletas.preco_num": "preco",
                "atletas.variacao_num": "precoVariacao",
                "PI": "PE",
                "DS": "RB",
            },
            inplace=True,
        )

        partidas_df = self.get_partidas()
        house_by_rounds = partidas_df[
            ["partidaID", "rodada", "clubeMandanteID"]
        ].rename(columns={"clubeMandanteID": "clubeID"})
        visitor_by_rounds = partidas_df[
            ["partidaID", "rodada", "clubeVisitanteID"]
        ].rename(columns={"clubeVisitanteID": "clubeID"})
        club_by_rounds = pd.concat([house_by_rounds, visitor_by_rounds])

        club_by_rounds["rodada"] = club_by_rounds["rodada"].astype(int)
        club_by_rounds["clubeID"] = club_by_rounds["clubeID"].astype(int)
        scouts_df["rodada"] = scouts_df["rodada"].astype(int)
        scouts_df["clubeID"] = scouts_df["clubeID"].astype(int)

        scouts_df = scouts_df.merge(
            right=club_by_rounds,
            on=["rodada", "clubeID"],
        )

        posicoes_df = self.get_posicoes("2020")
        scouts_df = scouts_df.merge(
            right=posicoes_df,
            left_on="atletas.posicao_id",
            right_on="abreviacao",
        )
        scouts_df["temporada"] = 2020
        scouts_df.drop_duplicates(
            subset=["partidaID", "atletaID", "clubeID"],
            keep="first",
            inplace=True,
        )

        return scouts_df

    def get_partidas(self):
        partidas_df = pd.read_csv(f"{self.input_path}/2020/partidas/1.csv", dtype=str)
        partidas_df.rename(
            columns={
                "home_team": "clubeMandanteID",
                "away_team": "clubeVisitanteID",
                "home_score": "mandantePlacar",
                "away_score": "visitantePlacar",
                "round": "rodada",
            },
            inplace=True,
        )

        def get_resultado(row):
            if row["mandantePlacar"] > row["visitantePlacar"]:
                return "Casa"
            elif row["mandantePlacar"] < row["visitantePlacar"]:
                return "Visitante"
            else:
                return "Empate"

        partidas_df.drop_duplicates(inplace=True)
        partidas_df["resultado"] = partidas_df.apply(get_resultado, axis=1)
        partidas_df["temporada"] = 2020
        partidas_df["partidaID"] = (
            partidas_df.temporada.astype(str)
            + partidas_df.rodada.astype(str)
            + partidas_df.clubeMandanteID.astype(str)
            + partidas_df.clubeVisitanteID.astype(str)
            + partidas_df.mandantePlacar.astype(str)
            + partidas_df.visitantePlacar.astype(str)
        )

        return partidas_df

    def get_atletas(self):
        file_list = glob(f"{self.input_path}/2020/rodada/*.csv")
        atletas_df = pd.DataFrame()
        for file in file_list:
            tmp_df = pd.read_csv(file, dtype=str)
            atletas_df = pd.concat([atletas_df, tmp_df], sort=True)

        atletas_df.rename(
            columns={
                "atletas.atleta_id": "atletaID",
                "atletas.apelido": "apelido",
            },
            inplace=True,
        )

        atletas_df = atletas_df[["atletaID", "apelido"]]
        atletas_df.drop_duplicates("atletaID", keep="first", inplace=True)
        atletas_df["temporada"] = 2020
        return atletas_df
