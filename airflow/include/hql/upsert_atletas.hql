MSCK REPAIR TABLE trusted.atletas;

TRUNCATE TABLE refined.atletas;

INSERT INTO refined.atletas
SELECT
    atletaID,
    apelido,
    temporada
FROM (
    SELECT
        atletaID,
        apelido,
        temporada,
        ROW_NUMBER() OVER(
            PARTITION BY atletaid
            ORDER BY temporada DESC
        ) as rownumber
    FROM trusted.atletas
) trusted_row_numbers
WHERE rownumber = 1;

MSCK REPAIR TABLE refined.atletas;
