CREATE OR REPLACE VIEW refined.partidas_full (
    temporada           COMMENT 'Temporada da partida',
    rodada              COMMENT 'Rodada em que a partida ocorreu',
    mandante            COMMENT 'Nome do clube mandante',
    placarMandate       COMMENT 'Placar do clube mandante',
    placarVisitante     COMMENT 'Placar do clube visitante',
    visitante           COMMENT 'Nome do clube visitante',
    resultado           COMMENT 'Resultado final da partida ["Casa", "Visitante", "Empate"]'
)
COMMENT 'View relacionando nomes de clubes e partidas do schema refined'
AS SELECT
    partidas.temporada AS temporada,
    partidas.rodada AS rodada,
    clubemandante.nome AS mandante,
    partidas.mandanteplacar AS placarMandante,
    partidas.visitanteplacar AS placarVisitante,
    clubevisitante.nome AS visitante,
    partidas.resultado AS resultado
FROM refined.partidas AS partidas
LEFT JOIN refined.clubes AS clubemandante
    ON partidas.clubemandanteid = clubemandante.clubeid
LEFT JOIN refined.clubes AS clubevisitante
    ON partidas.clubevisitanteid = clubevisitante.clubeid;

CREATE OR REPLACE VIEW refined.scouts_full (
    temporada       COMMENT 'Temporada da partida',
    rodada          COMMENT 'Rodada em que a partida ocorreu',
    clube           COMMENT 'Nome do clube',
    jogador         COMMENT 'Nome do atleta',
    posicao         COMMENT 'Abreviação da posição',
    pontos          COMMENT 'Pontuação do atleta nesta rodada',
    media           COMMENT 'Média de pontos do atleta até está rodada (inclue rodada atual)',
    preco           COMMENT 'Preço do atleta nesta rodada',
    variacao        COMMENT 'Variação do preço da rodada passada para está',
    FS              COMMENT 'Faltas sofridas',
    PE              COMMENT 'Passes errados',
    A               COMMENT 'Assistências',
    FT              COMMENT 'Finalizações na trave',
    FD              COMMENT 'Finalizações defendidas',
    FF              COMMENT 'Finalizações para fora',
    G               COMMENT 'Gols',
    I               COMMENT 'Impedimentos',
    PP              COMMENT 'Penaltis perdidos',
    RB              COMMENT 'Roubadas de bola',
    FC              COMMENT 'Faltas cometidas',
    GC              COMMENT 'Gols contras',
    CA              COMMENT 'Cartões Amarelos',
    CV              COMMENT 'Cartões Vermelhos',
    SG              COMMENT 'Jogo sem sofrer gols',
    DD              COMMENT 'Defesas dificeis',
    DP              COMMENT 'Defesa de penaltis',
    GS              COMMENT 'Gols sofridos'
)
COMMENT 'View relacionando atletas, clubes e scouts do schema refined'
AS SELECT
    partidas.temporada AS temporada,
    partidas.rodada AS rodada,
    clubes.nome AS clube,
    atletas.apelido AS jogador,
    UPPER(posicoes.abreviacao) AS posicao,
    scouts.pontos AS pontos,
    scouts.pontosmedia AS media,
    scouts.preco AS preco,
    scouts.precoVariacao AS variacao,
    scouts.FS AS FS,
    scouts.PE AS PE,
    scouts.A AS A,
    scouts.FT AS FT,
    scouts.FD AS FD,
    scouts.FF AS FF,
    scouts.G AS G,
    scouts.I AS I,
    scouts.PP AS PP,
    scouts.RB AS RB,
    scouts.FC AS FC,
    scouts.GC AS GC,
    scouts.CA AS CA,
    scouts.CV AS CV,
    scouts.SG AS SG,
    scouts.DD AS DD,
    scouts.DP AS DP,
    scouts.GS AS GS
FROM refined.scouts AS scouts
LEFT JOIN refined.partidas AS partidas
    ON scouts.partidaid = partidas.partidaid
LEFT JOIN refined.clubes AS clubes
    ON scouts.clubeid = clubes.clubeid
LEFT JOIN refined.atletas AS atletas
    ON scouts.atletaid = atletas.atletaid
LEFT JOIN refined.posicoes AS posicoes
    ON scouts.posicaoid = posicoes.posicaoid;
