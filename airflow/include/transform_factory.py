from transformers import (
    AbstractTransformer,
    Transformer2014,
    Transformer2015,
    Transformer2016,
    Transformer2017,
    Transformer2018,
    Transformer2019,
    Transformer2020,
)


class TransformFactory(AbstractTransformer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.transformers = {
            "2014": Transformer2014,
            "2015": Transformer2015,
            "2016": Transformer2016,
            "2017": Transformer2017,
            "2018": Transformer2018,
            "2019": Transformer2019,
            "2020": Transformer2020,
        }

    def get_scouts(self, year):
        transformer = self.transformers[year](self.input_path)
        scouts_df = transformer.get_scouts()
        self.write_parquet(
            df=scouts_df,
            schema="scouts",
            partition_by=year,
        )

    def get_partidas(self, year):
        transformer = self.transformers[year](self.input_path)
        partidas_df = transformer.get_partidas()
        self.write_parquet(
            df=partidas_df,
            schema="partidas",
            partition_by=year,
        )

    def get_atletas(self, year):
        transformer = self.transformers[year](self.input_path)
        atletas_df = transformer.get_atletas()
        self.write_parquet(
            df=atletas_df,
            schema="atletas",
            partition_by=year,
        )

    def get_clubes(self, year):
        clubes_df = super().get_clubes(year)
        self.write_parquet(
            df=clubes_df,
            schema="clubes",
            partition_by=None,
        )

    def get_posicoes(self, year):
        posicoes_df = super().get_posicoes(year)
        self.write_parquet(
            df=posicoes_df,
            schema="posicoes",
            partition_by=None,
        )
