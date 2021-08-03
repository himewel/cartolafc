import pandas as pd

from . import AbstractTransformer


class Transformer2014(AbstractTransformer):
    def get_scouts(self):
        clubes_df = pd.read_csv(f"{self.remote_path}/times_ids/1.csv", dtype=str)

        clubes_df = (
            clubes_df[["id", "cod.2018"]]
            .rename(columns={"cod.2018": "olderID", "id": "clubeID"})
            .drop_duplicates()
        )

        scouts_df = pd.read_csv(f"{self.remote_path}/scouts_raw/1.csv", dtype=str)
        scouts_df = scouts_df.merge(
            right=clubes_df,
            left_on="Clube",
            right_on="olderID",
            how="left",
        )

        scouts_df["temporada"] = 2014
        scouts_df["Participou"] = scouts_df["Participou"].astype(int)
        scouts_df["Rodada"] = scouts_df["Rodada"].astype(int)
        scouts_df = scouts_df[scouts_df.Participou > 0]
        scouts_df = scouts_df[scouts_df.Rodada > 0].rename(
            columns={
                "Atleta": "atletaID",
                "Partida": "partidaID",
                "Pontos": "pontos",
                "PontosMedia": "pontosMedia",
                "Preco": "preco",
                "PrecoVariacao": "precoVariacao",
                "Posicao": "posicaoID",
            },
        )
        scouts_df.drop_duplicates(
            subset=["partidaID", "atletaID", "clubeID"],
            keep="first",
            inplace=True,
        )

        return scouts_df

    def get_partidas(self):
        clubes_df = pd.read_csv(f"{self.remote_path}/times_ids/1.csv", dtype=str)

        clubes_df = (
            clubes_df[["id", "cod.2018"]]
            .rename(columns={"cod.2018": "olderID", "id": "clubeID"})
            .drop_duplicates()
        )

        partidas_df = pd.read_csv(f"{self.remote_path}/partidas_ids/1.csv", dtype=str)

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
        atletas_df = pd.read_csv(f"{self.remote_path}/jogadores/1.csv", dtype=str)

        atletas_df = atletas_df.drop_duplicates("ID", keep="first").rename(
            columns={
                "ID": "atletaID",
                "Apelido": "apelido",
                "PosicaoID": "posicaoID",
            }
        )
        atletas_df["temporada"] = 2014
        return atletas_df
