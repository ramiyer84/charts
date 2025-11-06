--loan level:

WITH fc05_pe_only AS (
  SELECT application_number, loan_id
  FROM fc05_records
  GROUP BY application_number, loan_id
  HAVING COUNT(*) = 1            -- exactly one cover row for that FC loan
     AND MAX(cover_type) = '090' -- and that single cover is PE
)
SELECT DISTINCT
    c.applicationfilefilenumber,
    c.loanid,
    sfl.application_id,
    sfl.macao_loan_id,           -- will be NULL per filter
    '090'   AS cover_type
FROM temp.dms_macao_coverid_isnull c
JOIN delta.sf_applications sfa
  ON c.applicationfilefilenumber = sfa.application_number
JOIN delta.sf_loans sfl
  ON c.loanid = sfl.loan_id
 AND sfa.opportunity_id = sfl.application_id
WHERE c.id IS NOT NULL
  AND sfl.macao_loan_id IS NULL
  AND EXISTS (                    -- there is a PE-only FC05 loan under this application
      SELECT 1
      FROM fc05_pe_only p
      WHERE p.application_number = sfa.application_number
  );



-- app level:

WITH fc05_app_pe_only AS (
  SELECT application_number
  FROM fc05_records
  GROUP BY application_number
  HAVING COUNT(*) = SUM(CASE WHEN cover_type = '090' THEN 1 ELSE 0 END)    -- no non-PE covers
     AND COUNT(*) = COUNT(DISTINCT CONCAT(loan_id, ':', cover_number))     -- one cover per loan
)
SELECT DISTINCT
    c.applicationfilefilenumber,
    c.loanid,
    sfl.application_id,
    sfl.macao_loan_id,
    '090' AS cover_type
FROM temp.dms_macao_coverid_isnull c
JOIN delta.sf_applications sfa
  ON c.applicationfilefilenumber = sfa.application_number
JOIN delta.sf_loans sfl
  ON c.loanid = sfl.loan_id
 AND sfa.opportunity_id = sfl.application_id
WHERE c.id IS NOT NULL
  AND sfl.macao_loan_id IS NULL
  AND sfa.application_number IN (SELECT application_number FROM fc05_app_pe_only);