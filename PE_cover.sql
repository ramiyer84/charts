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









-- Sanity check examples directly in FC05
SELECT application_number, loan_id, cover_type, COUNT(*) AS cnt
FROM fc05_records
WHERE application_number IN ('2001A087243','2001A026848')
GROUP BY application_number, loan_id, cover_type
ORDER BY application_number, loan_id;

-- Loans that have exactly one cover and that cover is 090
WITH fc05_pe_only AS (
  SELECT application_number, loan_id
  FROM fc05_records
  GROUP BY application_number, loan_id
  HAVING COUNT(*) = 1 AND MAX(cover_type) = '090'
)
SELECT *
FROM fc05_pe_only
WHERE application_number IN ('2001A087243','2001A026848');



WITH base AS (
  SELECT DISTINCT
      sfa.application_number,
      sfl.loan_id           AS mrt_loan_id,
      sfl.macao_loan_id,
      c.applicationfilefilenumber
  FROM temp.dms_macao_coverid_isnull c
  JOIN delta.sf_applications sfa
    ON c.applicationfilefilenumber = sfa.application_number
  JOIN delta.sf_loans sfl
    ON c.loanid = sfl.loan_id
   AND sfa.opportunity_id = sfl.application_id
  WHERE c.id IS NOT NULL
    AND sfl.macao_loan_id IS NULL
)
SELECT *
FROM base;




WITH base AS (
  SELECT DISTINCT
      sfa.application_number,
      sfl.loan_id AS mrt_loan_id
  FROM temp.dms_macao_coverid_isnull c
  JOIN delta.sf_applications sfa
    ON c.applicationfilefilenumber = sfa.application_number
  JOIN delta.sf_loans sfl
    ON c.loanid = sfl.loan_id
   AND sfa.opportunity_id = sfl.application_id
  WHERE c.id IS NOT NULL
    AND sfl.macao_loan_id IS NULL
),
fc05_pe_only AS (
  -- FC05 loans that have exactly one cover and it's 090
  SELECT application_number, loan_id AS fc_loan_id
  FROM fc05_records
  GROUP BY application_number, loan_id
  HAVING COUNT(*) = 1 AND MAX(cover_type) = '090'
)
SELECT DISTINCT
    b.application_number,
    b.mrt_loan_id,
    p.fc_loan_id,           -- loan(s) in FC05 that are PE-only
    '090' AS cover_type
FROM base b
JOIN fc05_pe_only p
  ON p.application_number = b.application_number;





WITH base AS (
  SELECT DISTINCT
      UPPER(LTRIM(RTRIM(c.applicationfilefilenumber))) AS app_key,
      sfl.loan_id AS mrt_loan_id
  FROM temp.dms_macao_coverid_isnull c
  JOIN delta.sf_applications sfa
    ON c.applicationfilefilenumber = sfa.application_number
  JOIN delta.sf_loans sfl
    ON c.loanid = sfl.loan_id
   AND sfa.opportunity_id = sfl.application_id
  WHERE c.id IS NOT NULL
    AND sfl.macao_loan_id IS NULL
),
fc05_pe_only AS (
  -- FC05 loans that have exactly one cover and it’s 090
  SELECT
      UPPER(LTRIM(RTRIM(application_number))) AS app_key,
      loan_id AS fc_loan_id
  FROM fc05_records
  GROUP BY UPPER(LTRIM(RTRIM(application_number))), loan_id
  HAVING COUNT(*) = 1 AND MAX(cover_type) = '090'
)
SELECT DISTINCT
    b.app_key AS application_number,
    b.mrt_loan_id,
    p.fc_loan_id,
    '090' AS cover_type
FROM base b
JOIN fc05_pe_only p
  ON p.app_key = b.app_key;