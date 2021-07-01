from transformers import AbstractTransformer, Transformer2014


class Transformer(AbstractTransformer):
    def __init__(self, path, schema_path=""):
        self.transformers = {
            "2014": Transformer2014,
        }
        super().__init__(path, schema_path)

    def get_scouts(self, year):
        transformer = self.transformers[year](path=self.path)
        scouts_df = transformer.get_scouts()
        self.write_parquet(
            df=scouts_df,
            schema="scouts",
            partition_by=year,
        )

    def get_partidas(self, year):
        transformer = self.transformers[year](path=self.path)
        partidas_df = transformer.get_partidas()
        self.write_parquet(
            df=partidas_df,
            schema="partidas",
            partition_by=year,
        )

    def get_atletas(self, year):
        transformer = self.transformers[year](path=self.path)
        atletas_df = transformer.get_atletas()
        self.write_parquet(
            df=atletas_df,
            schema="atletas",
            partition_by=year,
        )

    def get_clubes(self, year):
        transformer = self.transformers[year](path=self.path)
        clubes_df = transformer.get_clubes()
        self.write_parquet(
            df=clubes_df,
            schema="clubes",
            partition_by=year,
        )

    def get_posicoes(self, year):
        transformer = self.transformers[year](path=self.path)
        posicoes_df = transformer.get_posicoes()
        self.write_parquet(
            df=posicoes_df,
            schema="posicoes",
            partition_by=year,
        )


if __name__ == '__main__':
    transformer = Transformer(path="./data/raw")
    transformer.get_scouts("2014")
    transformer.get_partidas("2014")
    transformer.get_atletas("2014")
    transformer.get_clubes("2014")
    transformer.get_posicoes("2014")