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


class YearlyFactory(AbstractTransformer):
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

    def get_transformer(self, execution_date):
        year = str(execution_date.year)
        transformer = self.transformers[year](
            input_path=self.input_path,
            execution_date=execution_date,
        )
        return transformer

    def get_scouts(self, execution_date):
        transformer = self.get_transformer(execution_date)
        scouts_df = transformer.get_scouts()
        self.write_parquet(
            df=scouts_df,
            schema="scouts",
            partition_by=["temporada"],
        )

    def get_partidas(self, execution_date):
        transformer = self.get_transformer(execution_date)
        partidas_df = transformer.get_partidas()
        self.write_parquet(
            df=partidas_df,
            schema="partidas",
            partition_by=["temporada"],
        )

    def get_atletas(self, execution_date):
        transformer = self.get_transformer(execution_date)
        atletas_df = transformer.get_atletas()
        self.write_parquet(
            df=atletas_df,
            schema="atletas",
            partition_by=["temporada"],
        )

    def get_clubes(self, execution_date):
        transformer = self.get_transformer(execution_date)
        clubes_df = transformer.get_clubes()
        self.write_parquet(
            df=clubes_df,
            schema="clubes",
            partition_by=None,
        )

    def get_posicoes(self, execution_date):
        transformer = self.get_transformer(execution_date)
        posicoes_df = transformer.get_posicoes()
        self.write_parquet(
            df=posicoes_df,
            schema="posicoes",
            partition_by=None,
        )
