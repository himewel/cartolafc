import pandas as pd

from . import AbstractTransformer


class Transformer2017(AbstractTransformer):
    def get_scouts(self):
        clubes_df = pd.read_csv(f"{self.remote_path}/times_ids/1.csv", dtype=str)
        clubes_df = clubes_df[["id", "cod.2017", "nome.cartola"]].rename(
            columns={"cod.2017": "olderID", "id": "clubeID", "nome.cartola": "nome"}
        )

        scouts_df = pd.read_csv(f"{self.remote_path}/scouts_raw/1.csv", dtype=str)

        null_status = ["Nulo", "Contundido", "Suspenso"]
        scouts_df = scouts_df[~scouts_df["atletas.status_id"].isin(null_status)]

        scouts_df = scouts_df.merge(
            right=clubes_df,
            left_on="atletas.clube.id.full.name",
            right_on="nome",
        )

        scouts_df["Rodada"] = scouts_df["Rodada"].astype(int)
        scouts_df = scouts_df[scouts_df.Rodada > 0].rename(
            columns={
                "atletas.atleta_id": "atletaID",
                "Rodada": "rodada",
                "atletas.pontos_num": "pontos",
                "atletas.media_num": "pontosMedia",
                "atletas.preco_num": "preco",
                "atletas.variacao_num": "precoVariacao",
            },
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

        posicoes_df = self.get_posicoes()
        scouts_df = scouts_df.merge(
            right=posicoes_df,
            left_on="atletas.posicao_id",
            right_on="abreviacao",
        )
        scouts_df["temporada"] = 2017
        scouts_df.drop_duplicates(
            subset=["partidaID", "atletaID", "clubeID"],
            keep="first",
            inplace=True,
        )

        return scouts_df

    def get_partidas(self):
        clubes_df = pd.read_csv(f"{self.remote_path}/times_ids/1.csv", dtype=str)
        clubes_df = clubes_df[["id", "cod.2017", "nome.cbf"]].rename(
            columns={"cod.2017": "olderID", "id": "clubeID", "nome.cbf": "nome"}
        )

        partidas_df = pd.read_csv(f"{self.remote_path}/partidas/1.csv", dtype=str)
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
        partidas_df["temporada"] = 2017
        partidas_df.rename(
            columns={
                "clubeID": "clubeMandanteID",
                "clubeID_visitante": "clubeVisitanteID",
                "round": "rodada",
            },
            inplace=True,
        )

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
        atletas_df = pd.read_csv(f"{self.remote_path}/jogadores/1.csv", dtype=str)
        atletas_df = atletas_df.drop_duplicates("AtletaID", keep="first")
        atletas_df = atletas_df.rename(
            columns={
                "AtletaID": "atletaID",
                "Apelido": "apelido",
            },
        )
        atletas_df["temporada"] = 2017
        return atletas_df
