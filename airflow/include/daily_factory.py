from transformers import AbstractTransformer, Transformer2021


class DailyFactory(AbstractTransformer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.transformers = {
            "2021": Transformer2021,
        }

    def get_transformer(self, execution_date):
        year = str(execution_date.year)
        transformer = self.transformers[year](
            input_path=self.input_path,
            execution_date=execution_date,
        )
        return transformer

    def get_scouts(self, execution_date, next_execution_date):
        transformer = self.get_transformer(execution_date)
        scouts_df = transformer.get_scouts(next_execution_date)
        self.write_parquet(
            df=scouts_df,
            schema="scouts",
            partition_by=["temporada"],
        )

    def get_partidas(self, execution_date, next_execution_date):
        transformer = self.get_transformer(execution_date)
        partidas_df = transformer.get_partidas(next_execution_date)
        self.write_parquet(
            df=partidas_df,
            schema="partidas",
            partition_by=["temporada"],
        )

    def get_atletas(self, execution_date, next_execution_date):
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
