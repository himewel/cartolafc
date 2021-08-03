import pandas as pd

from . import AbstractTransformer


class Transformer2015(AbstractTransformer):
    def get_scouts(self):
        hdfs = self.get_conn()
        clubes_df = pd.read_csv(f"{self.remote_path}/times_ids/1.csv", dtype=str)
        clubes_df = (
            clubes_df[["id", "cod.2018"]]
            .rename(columns={"cod.2018": "olderID", "id": "clubeID"})
            .drop_duplicates()
        )

        scouts_df = pd.read_csv(f"{self.remote_path}/scouts_raw/1.csv", dtype=str)
        scouts_df = scouts_df.merge(
            right=clubes_df,
            left_on="ClubeID",
            right_on="olderID",
        )

        scouts_df["Rodada"] = scouts_df["Rodada"].astype(int)
        scouts_df = scouts_df[scouts_df.Rodada > 0].rename(
            columns={
                "AtletaID": "atletaID",
                "Rodada": "rodada",
                "Pontos": "pontos",
                "PontosMedia": "pontosMedia",
                "Preco": "preco",
                "PrecoVariacao": "precoVariacao",
            }
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

        scouts_df = scouts_df.merge(right=club_by_rounds, on=["rodada", "clubeID"])

        atletas_df = self.get_atletas()
        scouts_df = scouts_df.merge(right=atletas_df, on="atletaID")
        scouts_df["temporada"] = 2015
        scouts_df.drop_duplicates(
            subset=["partidaID", "atletaID", "clubeID"],
            keep="first",
            inplace=True,
        )

        return scouts_df

    def get_partidas(self):
        hdfs = self.get_conn()
        clubes_df = pd.read_csv(f"{self.remote_path}/times_ids/1.csv", dtype=str)
        clubes_df = (
            clubes_df[["id", "cod.2018"]]
            .rename(columns={"cod.2018": "olderID", "id": "clubeID"})
            .drop_duplicates()
        )

        partidas_df = pd.read_csv(f"{self.remote_path}/partidas_ids/1.csv", dtype=str)
        partidas_df = partidas_df.merge(
            right=clubes_df,
            left_on="CasaID",
            right_on="olderID",
            suffixes=(None, "_mandante"),
        )
        partidas_df = partidas_df.merge(
            right=clubes_df,
            left_on="VisitanteID",
            right_on="olderID",
            suffixes=(None, "_visitante"),
        )

        partidas_df["temporada"] = 2015
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
        atletas_df["temporada"] = 2015
        return atletas_df
