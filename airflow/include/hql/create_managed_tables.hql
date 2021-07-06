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
);
