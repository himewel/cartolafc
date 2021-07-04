import uuid

import pandas as pd

from . import AbstractTransformer


class Transformer2017(AbstractTransformer):
    def get_scouts(self):
        clubes_df = pd.read_csv(f"{self.input_path}/2017/times_ids/1.csv")
        clubes_df = clubes_df[["id", "cod.2017", "nome.cartola"]].rename(
            columns={"cod.2017": "olderID", "id": "clubeID", "nome.cartola": "nome"}
        )

        scouts_df = pd.read_csv(f"{self.input_path}/2017/scouts_raw/1.csv")
        scouts_df = scouts_df.merge(
            right=clubes_df,
            left_on="atletas.clube.id.full.name",
            right_on="nome",
        )

        scouts_df.rename(
            columns={
                "atletas.atleta_id": "atletaID",
                "Rodada": "rodada",
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
        clubes_df = pd.read_csv(f"{self.input_path}/2017/times_ids/1.csv")
        clubes_df = clubes_df[["id", "cod.2017", "nome.cbf"]].rename(
            columns={"cod.2017": "olderID", "id": "clubeID", "nome.cbf": "nome"}
        )

        partidas_df = pd.read_csv(f"{self.input_path}/2017/partidas/1.csv")
        partidas_df = partidas_df.merge(
            right=clubes_df,
            left_on="home_team",
            right_on="nome",
            suffixes=(None, "_mandante"),
        )
        partidas_df = partidas_df.merge(
            right=clubes_df,
            left_on="away_team",
            right_on="nome",
            suffixes=(None, "_visitante"),
        )

        partidas_df["mandantePlacar"] = (
            partidas_df["score"].str.split(" x ").str[0].astype(int)
        )
        partidas_df["visitantePlacar"] = (
            partidas_df["score"].str.split(" x ").str[-1].astype(int)
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

        partidas_df["temporada"] = 2017
        partidas_df.rename(
            columns={
                "clubeID": "clubeMandanteID",
                "clubeID_visitante": "clubeVisitanteID",
                "round": "rodada",
            },
            inplace=True,
        )

        return partidas_df

    def get_atletas(self):
        clubes_df = pd.read_csv(f"{self.input_path}/2017/times_ids/1.csv")
        clubes_df = (
            clubes_df[["id", "cod.2017"]]
            .rename(columns={"cod.2017": "olderID", "id": "clubeID"})
            .drop_duplicates()
        )

        atletas_df = pd.read_csv(f"{self.input_path}/2017/jogadores/1.csv")
        atletas_df = atletas_df.merge(
            right=clubes_df,
            left_on="ClubeID",
            right_on="olderID",
        )

        atletas_df = atletas_df.drop_duplicates().rename(
            columns={
                "AtletaID": "atletaID",
                "Apelido": "apelido",
                "PosicaoID": "posicaoID",
            }
        )

        return atletas_df
