SELECT
    temporada,
    rodada,
    clube,
    jogador,
    pontos,
    media
FROM (
    SELECT
        scouts.temporada,
        partidas.rodada,
        clubes.nome AS clube,
        atletas.apelido AS jogador,
        scouts.pontos,
        scouts.pontosmedia AS media,
        RANK() OVER (
            PARTITION BY scouts.temporada
            ORDER BY scouts.pontos DESC
        ) AS scoutrank
    FROM trusted.scouts AS scouts
    JOIN trusted.clubes AS clubes
        ON clubes.clubeid  = scouts.clubeid
    JOIN trusted.partidas AS partidas
        ON partidas.partidaid = scouts.partidaid
    JOIN trusted.atletas AS atletas
        ON atletas.atletaid = scouts.atletaid
) ranked_scouts
WHERE ranked_scouts.scoutrank = 1
ORDER BY ranked_scouts.pontos DESC
