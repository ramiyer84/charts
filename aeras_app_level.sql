SELECT
    sfa.application_number,
    sf.macao_loan_id,
    MAX(
        CASE rpp."Level AERAS RPP migration"
            WHEN 'Niveau 3' THEN 3
            WHEN 'Niveau 2' THEN 2
            WHEN 'Niveau 1' THEN 1
            ELSE 1
        END
    ) AS application_aeras_level
FROM
    delta.sf_applications sfa
INNER JOIN
    delta.sf_loans sf ON sf.application_id = sfa.opportunity_id
LEFT JOIN
    temp.rpp rpp ON sf.scheme_name = rpp.rpp
WHERE
    sf.loan_id IS NOT NULL
GROUP BY
    sfa.application_number,
    sf.macao_loan_id
ORDER BY
    sfa.application_number;