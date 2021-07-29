SELECT
    temporada,
    rodada,
    clube,
    jogador,
    pontos,
    media,
    posicao
FROM (
    SELECT
        scouts.temporada,
        partidas.rodada,
        clubes.nome AS clube,
        atletas.apelido AS jogador,
        scouts.pontos AS pontos,
        scouts.pontosmedia AS media,
        posicoes.abreviacao AS posicao,
        RANK() OVER (
            PARTITION BY scouts.posicaoid
            ORDER BY scouts.pontos DESC
        ) AS scoutsrank
    FROM trusted.scouts
    JOIN trusted.clubes
        ON scouts.clubeid = clubes.clubeid
    JOIN trusted.partidas
        ON scouts.partidaid = partidas.partidaid
    JOIN trusted.atletas
        ON scouts.atletaid = atletas.atletaid
    JOIN trusted.posicoes
        ON scouts.posicaoid = posicoes.posicaoid
) ranked_players
WHERE
    (posicao IN ('gol', 'tec') AND scoutsrank = 1) OR
    (posicao IN ('mei', 'ata') AND scoutsrank <= 3) OR
    (posicao IN ('zag', 'lat') AND scoutsrank <= 2)
