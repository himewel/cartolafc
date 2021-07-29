MSCK REPAIR TABLE curated.atletas;

TRUNCATE TABLE trusted.atletas;

INSERT INTO trusted.atletas (
    atletaID,
    apelido,
    temporada
)
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
    FROM curated.atletas
) trusted_row_numbers
WHERE rownumber = 1;

MSCK REPAIR TABLE trusted.atletas;
