SELECT
    temporada,
    COUNT(1) AS scouts,
    COUNT(DISTINCT(atletaid)) AS atletas,
    COUNT(DISTINCT(partidaid)) AS partidas
FROM trusted.scouts
GROUP BY temporada
