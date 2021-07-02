import pandas as pd

from . import AbstractTransformer


class Transformer2014(AbstractTransformer):
    def get_scouts(self):
        clubes_df = pd.read_csv(f"{self.path}/2014/times_ids/1.csv")
        clubes_df = (
            clubes_df[["id", "cod.2018"]]
            .rename(columns={"cod.2018": "olderID", "id": "clubeID"})
            .drop_duplicates()
        )

        scouts_df = pd.read_csv(f"{self.path}/2014/scouts_raw/1.csv")
        scouts_df = scouts_df.merge(
            right=clubes_df,
            left_on="Clube",
            right_on="olderID",
            how="left",
        )

        scouts_df = scouts_df[scouts_df.Rodada > 0].rename(
            columns={
                "Atleta": "atletaID",
                "Partida": "partidaID",
                "Pontos": "pontos",
                "PontosMedia": "pontosMedia",
                "Preco": "preco",
                "PrecoVariacao": "precoVariacao",
            },
        )

        return scouts_df

    def get_partidas(self):
        clubes_df = pd.read_csv(f"{self.path}/2014/times_ids/1.csv")
        clubes_df = (
            clubes_df[["id", "cod.2018"]]
            .rename(columns={"cod.2018": "olderID", "id": "clubeID"})
            .drop_duplicates()
        )

        partidas_df = pd.read_csv(f"{self.path}/2014/partidas_ids/1.csv")
        partidas_df = partidas_df.merge(
            right=clubes_df,
            left_on="Casa",
            right_on="olderID",
            suffixes=(None, "_mandante"),
        )
        partidas_df = partidas_df.merge(
            right=clubes_df,
            left_on="Visitante",
            right_on="olderID",
            suffixes=(None, "_visitante"),
        )

        partidas_df["temporada"] = 2014
        partidas_df.rename(
            columns={
                "ID": "partidaID",
                "clubeID": "clubeMandanteID",
                "clubeID_visitante": "clubeVisitanteID",
                "PlacarCasa": "mandantePlacar",
                "PlacarVisitante": "visitantePlacar",
                "Rodada": "rodada",
                "Resultado": "resultado",
            },
            inplace=True,
        )
        return partidas_df

    def get_atletas(self):
        clubes_df = pd.read_csv(f"{self.path}/2014/times_ids/1.csv")
        clubes_df = (
            clubes_df[["id", "cod.2018"]]
            .rename(columns={"cod.2018": "olderID", "id": "clubeID"})
            .drop_duplicates()
        )

        atletas_df = pd.read_csv(f"{self.path}/2014/jogadores/1.csv")
        atletas_df = atletas_df.merge(
            right=clubes_df,
            left_on="ClubeID",
            right_on="olderID",
        )

        atletas_df = atletas_df.drop_duplicates().rename(
            columns={
                "ID": "atletaID",
                "Apelido": "apelido",
                "PosicaoID": "posicaoID",
            }
        )
        return atletas_df

    def get_clubes(self):
        clubes_df = pd.read_csv(f"{self.path}/2014/times_ids/1.csv")
        clubes_df = (
            clubes_df[["id", "nome.cbf", "abreviacao"]]
            .rename(columns={"id": "clubeID", "nome.cbf": "nome"})
            .drop_duplicates(subset=["clubeID"], keep="last")
        )
        return clubes_df

    def get_posicoes(self):
        posicoes_df = pd.read_csv(f"{self.path}/2014/posicoes_ids/1.csv")
        posicoes_df.rename(
            columns={
                "Cod": "posicaoID",
                "Position": "nome",
                "abbr": "abreviacao",
            },
            inplace=True,
        )
        return posicoes_df
