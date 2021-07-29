import fsspec
import pandas as pd

from . import AbstractTransformer


class Transformer2018(AbstractTransformer):
    def get_scouts(self):
        hdfs = self.get_conn()
        clubes_df = pd.read_csv(f"{hdfs}/raw/2018/times_ids/1.csv", dtype=str)
        clubes_df = (
            clubes_df[["id", "cod.2018", "nome.cartola"]]
            .drop_duplicates()
            .rename(
                columns={
                    "cod.2018": "olderID",
                    "id": "clubeID",
                    "nome.cartola": "nome",
                }
            )
        )

        scouts_df = pd.DataFrame()
        with fsspec.open_files(f"{hdfs}/raw/2018/rodada/*.csv") as file_list:
            drop_subset = ["atletas.clube_id", "atletas.atleta_id", "atletas.rodada_id"]
            for file in file_list:
                tmp_df = pd.read_csv(file, dtype=str).drop_duplicates(
                    subset=drop_subset, keep="first"
                )
                scouts_df = pd.concat([scouts_df, tmp_df], sort=True)

        null_status = ["Nulo", "Contundido", "Suspenso"]
        scouts_df = scouts_df[~scouts_df["atletas.status_id"].isin(null_status)]

        scouts_df = scouts_df.merge(
            right=clubes_df,
            left_on="atletas.clube.id.full.name",
            right_on="nome",
        )

        scouts_df.rename(
            columns={
                'atletas.atleta_id': "atletaID",
                'atletas.rodada_id': "rodada",
                'atletas.pontos_num': "pontos",
                'atletas.media_num': "pontosMedia",
                'atletas.preco_num': "preco",
                'atletas.variacao_num': "precoVariacao",
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

        scouts_df = scouts_df.merge(right=club_by_rounds, on=["rodada", "clubeID"])

        posicoes_df = self.get_posicoes("2018")
        scouts_df = scouts_df.merge(
            right=posicoes_df,
            left_on="atletas.posicao_id",
            right_on="abreviacao",
        )
        scouts_df["temporada"] = 2018
        scouts_df.drop_duplicates(
            subset=["partidaID", "atletaID", "clubeID"],
            keep="first",
            inplace=True,
        )

        return scouts_df

    def get_partidas(self):
        hdfs = self.get_conn()
        clubes_df = pd.read_csv(f"{hdfs}/raw/2018/times_ids/1.csv", dtype=str)
        clubes_df = clubes_df[["id", "cod.2018", "nome.cbf"]].rename(
            columns={"cod.2018": "olderID", "id": "clubeID", "nome.cbf": "nome"},
        )

        partidas_df = pd.read_csv(f"{hdfs}/raw/2018/partidas/1.csv", dtype=str)
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
        partidas_df["temporada"] = 2018
        partidas_df.rename(
            columns={
                "clubeID": "clubeMandanteID",
                "clubeID_visitante": "clubeVisitanteID",
                "round": "rodada",
            },
            inplace=True,
        )
        partidas_df.drop_duplicates(
            subset=["clubeMandanteID", "clubeVisitanteID"],
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
        hdfs = self.get_conn()
        atletas_df = pd.read_csv(f"{hdfs}/raw/2018/jogadores/1.csv", dtype=str)
        atletas_df = atletas_df.drop_duplicates("atletas.atleta_id", keep="first")
        atletas_df = atletas_df.rename(
            columns={
                "atletas.atleta_id": "atletaID",
                "atletas.apelido": "apelido",
            },
        )
        atletas_df["temporada"] = 2018
        return atletas_df
