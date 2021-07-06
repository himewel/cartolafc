CREATE EXTERNAL TABLE IF NOT EXISTS external_atletas (
    atletaID int,
    clubeID int,
    apelido string,
    posicaoID int
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/atletas';

CREATE EXTERNAL TABLE IF NOT EXISTS external_clubes (
    clubeID int,
    nome string,
    abreviacao string
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/clubes';

CREATE EXTERNAL TABLE IF NOT EXISTS external_partidas (
    partidaID string,
    rodada int,
    clubeMandanteID int,
    clubeVisitanteID int,
    mandantePlacar int,
    visitantePlacar int,
    resultado string,
    temporada int
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/partidas';

CREATE EXTERNAL TABLE IF NOT EXISTS external_posicoes (
    posicaoID int,
    nome string,
    abreviacao string
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/posicoes';

CREATE EXTERNAL TABLE IF NOT EXISTS external_scouts (
    atletaID int,
    partidaID string,
    clubeID int,
    pontos double,
    pontosMedia double,
    preco double,
    precoVariacao double,
    FS double,
    PE double,
    A double,
    FT double,
    FD double,
    FF double,
    G double,
    I double,
    PP double,
    RB double,
    FC double,
    GC double,
    CA double,
    CV double,
    SG double,
    DD double,
    DP double,
    GS double
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/scouts';
