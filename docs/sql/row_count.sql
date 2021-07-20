SELECT
    temporada,
    COUNT(1) AS scouts,
    COUNT(DISTINCT(atletaid)) AS atletas,
    COUNT(DISTINCT(partidaid)) AS partidas
FROM refined.scouts
GROUP BY temporada
