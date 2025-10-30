-- 1) All DMS covers for the loans in scope
SELECT COUNT(*)
FROM fc05_records c
WHERE c.loan_id IN (<your loan set>);

-- 2) Same, but only where application_number matches your temp table
SELECT COUNT(*)
FROM fc05_records c
JOIN temp.dms_macao_coverid_isnull n
  ON n.applicationfilefilenumber = c.application_number
 AND n.loanid = c.loan_id;

-- 3) Same as (2) but group and keep only those with exactly 1 cover
SELECT COUNT(*)
FROM (
  SELECT c.application_number, c.loan_id
  FROM fc05_records c
  JOIN temp.dms_macao_coverid_isnull n
    ON n.applicationfilefilenumber = c.application_number
   AND n.loanid = c.loan_id
  GROUP BY c.application_number, c.loan_id
  HAVING COUNT(*) = 1
) x;