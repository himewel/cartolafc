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
    pontos float,
    pontosMedia float,
    preco float,
    precoVariacao float,
    FS float,
    PE float,
    A float,
    FT float,
    FD float,
    FF float,
    G float,
    I float,
    PP float,
    RB float,
    FC float,
    GC float,
    CA float,
    CV float,
    SG float,
    DD float,
    DP float,
    GS float
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/scouts';
