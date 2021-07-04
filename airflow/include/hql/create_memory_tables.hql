CREATE TABLE IF NOT EXISTS atletas (
    atletaID int,
    clubeID int,
    apelido string,
    posicaoID int
);

CREATE TABLE IF NOT EXISTS clubes (
    clubeID int,
    nome string,
    abreviacao string
);

CREATE TABLE IF NOT EXISTS partidas (
    partidaID string,
    rodada int,
    clubeMandanteID int,
    clubeVisitanteID int,
    mandantePlacar int,
    visitantePlacar int,
    resultado string,
    temporada int
);

CREATE TABLE IF NOT EXISTS posicoes (
    posicaoID int,
    nome string,
    abreviacao string
);

CREATE TABLE IF NOT EXISTS scouts (
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
);
