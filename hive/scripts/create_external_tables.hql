CREATE SCHEMA IF NOT EXISTS curated
COMMENT 'Expoe tabelas da camada curated do data lake';

CREATE EXTERNAL TABLE IF NOT EXISTS curated.atletas (
    atletaID    INT     COMMENT 'ID do atleta',
    apelido     STRING  COMMENT 'Apelido do atleta'
)
COMMENT 'Tabela espelhando camada curated do data lake com dados de atletas'
PARTITIONED BY (
    temporada   INT     COMMENT 'Temporada de inclusão'
)
STORED AS PARQUET
LOCATION 'hdfs:/curated/atletas';

CREATE EXTERNAL TABLE IF NOT EXISTS curated.clubes (
    clubeID     INT     COMMENT 'ID do clube',
    nome        STRING  COMMENT 'Nome do clube',
    abreviacao  STRING  COMMENT 'Abreviação do clube'
)
COMMENT 'Tabela espelhando camada curated do data lake com dados de clubes'
STORED AS PARQUET
LOCATION 'hdfs:/curated/clubes';

CREATE EXTERNAL TABLE IF NOT EXISTS curated.partidas (
    partidaID           STRING  COMMENT 'UUID da partida',
    rodada              INT     COMMENT 'Rodada em que a partida ocorreu',
    clubeMandanteID     INT     COMMENT 'ID do clube mandante',
    clubeVisitanteID    INT     COMMENT 'ID do clube visitante',
    mandantePlacar      INT     COMMENT 'Placar do clube mandante',
    visitantePlacar     INT     COMMENT 'Placar do clube visitante',
    resultado           STRING  COMMENT 'Resultado final da partida ["Casa", "Visitante", "Empate"]'
)
COMMENT 'Tabela espelhando camada curated do data lake com dados de partidas'
PARTITIONED BY (
    temporada           INT     COMMENT 'Temporada da partida'
)
STORED AS PARQUET
LOCATION 'hdfs:/curated/partidas';

CREATE EXTERNAL TABLE IF NOT EXISTS curated.posicoes (
    posicaoID   INT     COMMENT 'ID da posição',
    nome        STRING  COMMENT 'Nome da posição',
    abreviacao  STRING  COMMENT 'Abreviação da posição'
)
COMMENT 'Tabela espelhando camada curated do data lake com dados de posicoes'
STORED AS PARQUET
LOCATION 'hdfs:/curated/posicoes';

CREATE EXTERNAL TABLE IF NOT EXISTS curated.scouts (
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
COMMENT 'Tabela espelhando camada curated do data lake com dados de scouts'
PARTITIONED BY (
    temporada       INT     COMMENT 'Temporada da partida'
)
STORED AS PARQUET
LOCATION 'hdfs:/curated/scouts';
