WITH mrt AS (
  SELECT
      n.applicationfilefilenumber      AS application_number,
      n.loanid,
      MIN(c.cover_id)                  AS mrt_cover_id,
      MIN(c.name)                      AS mrt_name,
      MIN(c.cover_type)                AS mrt_cover_type
  FROM temp.dms_macao_coverid_isnull n
  JOIN delta.dms_covers c
    ON n.loanid = c.loan_id
  WHERE n.comment = 'Decision present to migrate'
  GROUP BY n.applicationfilefilenumber, n.loanid
  HAVING COUNT(c.cover_type) = 1
),
macao AS (
  SELECT
      c.application_number,
      n.loanid,
      MIN(c.cover_id)                  AS macao_cover_id,
      MIN(c.cover_number)              AS macao_cover_number,
      MIN(c.cover_type)                AS macao_cover_type
  FROM temp.dms_macao_coverid_isnull n
  JOIN delta.sf_loans sfl
    ON sfl.loan_id = n.loanid
  JOIN fc05_records c
    ON sfl.macao_loan_id = c.loan_id
   AND n.applicationfilefilenumber = c.application_number
  WHERE n.comment = 'Decision present to migrate'
  GROUP BY c.application_number, n.loanid
  HAVING COUNT(c.cover_type) = 1
)
SELECT
    m.application_number,
    m.loanid,
    mrt.mrt_cover_id,
    mrt.mrt_name,
    mrt.mrt_cover_type,
    macao.macao_cover_id,
    macao.macao_cover_number,
    macao.macao_cover_type
FROM mrt
JOIN macao m
  ON m.loanid = mrt.loanid
 AND m.application_number = mrt.application_number;  -- drop this line if you only want to match on loanid