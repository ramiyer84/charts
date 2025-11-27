WITH loan_level AS (
    SELECT
        sfa.application_number,
        sf.macao_loan_id,
        sf.loan_id,
        rpp.rpp,
        rpp.level_aeras_rpp_migration,
        CASE
            WHEN rpp.level_aeras_rpp_migration = '3' THEN 3
            WHEN rpp.level_aeras_rpp_migration = '2' THEN 2
            WHEN rpp.level_aeras_rpp_migration = '1' THEN 1
            ELSE 1
        END AS dms_aeras_level
    FROM
        delta.sf_applications sfa
        INNER JOIN delta.sf_loans sf
            ON sf.application_id = sfa.opportunity_id
        LEFT JOIN temp.rpp rpp
            ON sf.scheme_name = rpp.rpp
    WHERE
        sf.loan_id IS NOT NULL
),

apps_with_mismatch AS (
    SELECT
        application_number
    FROM
        loan_level
    GROUP BY
        application_number
    HAVING
        COUNT(DISTINCT dms_aeras_level) > 1
)

SELECT
    l.application_number,
    l.macao_loan_id,
    l.loan_id,
    l.rpp,
    l.level_aeras_rpp_migration,
    l.dms_aeras_level
FROM
    loan_level l
    JOIN apps_with_mismatch a
        ON l.application_number = a.application_number
ORDER BY
    l.application_number,
    l.loan_id;