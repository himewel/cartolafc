CREATE SCHEMA IF NOT EXISTS trusted;

CREATE EXTERNAL TABLE IF NOT EXISTS trusted.atletas (
    atletaID    INT     COMMENT 'ID do atleta',
    apelido     STRING  COMMENT 'Apelido do atleta'
)
PARTITIONED BY (
    temporada   INT     COMMENT 'Temporada de inclusão'
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/atletas';

CREATE EXTERNAL TABLE IF NOT EXISTS trusted.clubes (
    clubeID     INT     COMMENT 'ID do clube',
    nome        STRING  COMMENT 'Nome do clube',
    abreviacao  STRING  COMMENT 'Abreviação do clube'
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/clubes';

CREATE EXTERNAL TABLE IF NOT EXISTS trusted.partidas (
    partidaID           STRING  COMMENT 'UUID da partida',
    rodada              INT     COMMENT 'Rodada em que a partida ocorreu',
    clubeMandanteID     INT     COMMENT 'ID do clube mandante',
    clubeVisitanteID    INT     COMMENT 'ID do clube visitante',
    mandantePlacar      INT     COMMENT 'Placar do clube mandante',
    visitantePlacar     INT     COMMENT 'Placar do clube visitante',
    resultado           STRING  COMMENT 'Resultado final da partida ["Casa", "Visitante", "Empate"]'
)
PARTITIONED BY (
    temporada           INT     COMMENT 'Temporada da partida'
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/partidas';

CREATE EXTERNAL TABLE IF NOT EXISTS trusted.posicoes (
    posicaoID   INT     COMMENT 'ID da posição',
    nome        STRING  COMMENT 'Nome da posição',
    abreviacao  STRING  COMMENT 'Abreviação da posição'
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/posicoes';

CREATE EXTERNAL TABLE IF NOT EXISTS trusted.scouts (
    partidaID       STRING  COMMENT 'UUID da partida',
    atletaID        INT     COMMENT 'ID do atleta',
    clubeID         INT     COMMENT 'ID do clube do atleta',
    posicaoID       INT     COMMENT 'ID da posição do atleta',
    pontos          DOUBLE  COMMENT 'Pontuação do atleta nesta rodada',
    pontosMedia     DOUBLE  COMMENT 'Média de pontos do atleta até está rodada (inclue rodada atual)',
    preco           DOUBLE  COMMENT 'Preço do atleta nesta rodada',
    precoVariacao   DOUBLE  COMMENT 'Variação do preço da rodada passada para está',
    FS              DOUBLE  COMMENT 'Faltas sofridas',
    PE              DOUBLE  COMMENT 'Passes errados',
    A               DOUBLE  COMMENT 'Assistências',
    FT              DOUBLE  COMMENT 'Finalizações na trave',
    FD              DOUBLE  COMMENT 'Finalizações defendidas',
    FF              DOUBLE  COMMENT 'Finalizações para fora',
    G               DOUBLE  COMMENT 'Gols',
    I               DOUBLE  COMMENT 'Impedimentos',
    PP              DOUBLE  COMMENT 'Penaltis perdidos',
    RB              DOUBLE  COMMENT 'Roubadas de bola',
    FC              DOUBLE  COMMENT 'Faltas cometidas',
    GC              DOUBLE  COMMENT 'Gols contras',
    CA              DOUBLE  COMMENT 'Cartões Amarelos',
    CV              DOUBLE  COMMENT 'Cartões Vermelhos',
    SG              DOUBLE  COMMENT 'Jogo sem sofrer gols',
    DD              DOUBLE  COMMENT 'Defesas dificeis',
    DP              DOUBLE  COMMENT 'Defesa de penaltis',
    GS              DOUBLE  COMMENT 'Gols sofridos'
)
PARTITIONED BY (
    temporada       INT     COMMENT 'Temporada da partida'
)
STORED AS PARQUET
LOCATION 'hdfs:/trusted/scouts';
