-- MSSQL
WITH combined AS (
    SELECT macaoCoverId
    FROM dbo.Coverages
    WHERE macaoCoverId IS NOT NULL

    UNION ALL

    SELECT macaoCoverId
    FROM dbo.Options
    WHERE macaoCoverId IS NOT NULL
)
SELECT 
    macaoCoverId,
    COUNT(*) AS mssql_count
FROM combined
GROUP BY macaoCoverId
ORDER BY macaoCoverId;





-- Postgres
-- adjust column name if it's camelCase (then quote it: "macaoCoverId")
SELECT 
    macao_cover_id AS macaoCoverId,
    COUNT(*) AS pg_count
FROM delta.dms_covers
WHERE macao_cover_id IS NOT NULL
GROUP BY macao_cover_id
ORDER BY macao_cover_id;