import uuid
from glob import glob

import pandas as pd

from . import AbstractTransformer


class Transformer2019(AbstractTransformer):
    def get_scouts(self):
        file_list = glob(f"{self.path}/2019/rodada/*.csv")
        scouts_df = pd.DataFrame()
        for file in file_list:
            tmp_df = pd.read_csv(file)
            scouts_df = pd.concat([scouts_df, tmp_df], sort=True)

        scouts_df.rename(
            columns={
                "atletas.clube_id": "clubeID",
                "atletas.atleta_id": "atletaID",
                "atletas.rodada_id": "rodada",
                "atletas.pontos_num": "pontos",
                "atletas.media_num": "pontosMedia",
                "atletas.preco_num": "preco",
                "atletas.variacao_num": "precoVariacao",
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

        scouts_df = scouts_df.merge(
            right=club_by_rounds,
            on=["rodada", "clubeID"],
        )

        return scouts_df

    def get_partidas(self):
        partidas_df = pd.read_csv(f"{self.path}/2019/partidas/1.csv")
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

        partidas_df["resultado"] = partidas_df.apply(get_resultado, axis=1)
        partidas_df["partidaID"] = [uuid.uuid4().hex for _ in range(len(partidas_df))]
        partidas_df["temporada"] = 2019

        return partidas_df

    def get_atletas(self):
        file_list = glob(f"{self.path}/2019/rodada/*.csv")
        atletas_df = pd.DataFrame()
        for file in file_list:
            tmp_df = pd.read_csv(file)
            atletas_df = pd.concat([atletas_df, tmp_df], sort=True)

        atletas_df.rename(
            columns={
                "atletas.clube_id": "clubeID",
                "atletas.atleta_id": "atletaID",
                "atletas.apelido": "apelido",
            },
            inplace=True,
        )

        posicoes_df = self.get_posicoes()
        atletas_df = atletas_df.merge(
            right=posicoes_df,
            left_on="atletas.posicao_id",
            right_on="abreviacao",
        )

        atletas_df = atletas_df[["atletaID", "clubeID", "posicaoID", "apelido"]]
        atletas_df.drop_duplicates(inplace=True)

        return atletas_df

    def get_clubes(self):
        clubes_df = pd.read_csv(f"{self.path}/2019/times_ids/1.csv")
        clubes_df = (
            clubes_df[["id", "nome.cbf", "abreviacao"]]
            .rename(columns={"id": "clubeID", "nome.cbf": "nome"})
            .drop_duplicates(subset=["clubeID"], keep="last")
        )
        return clubes_df

    def get_posicoes(self):
        posicoes_df = pd.read_csv(f"{self.path}/2019/posicoes_ids/1.csv")
        posicoes_df.rename(
            columns={
                "Cod": "posicaoID",
                "Position": "nome",
                "abbr": "abreviacao",
            },
            inplace=True,
        )
        return posicoes_df
