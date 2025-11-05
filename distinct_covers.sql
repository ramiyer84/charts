SELECT DISTINCT
    c.applicationfilefilenumber,
    c.loanid,
    sfl.application_id,
    sfl.macao_loan_id,
    fc05.cover_type,
    fc05.cover_number
FROM temp.dms_macao_coverid_isnull c
JOIN delta.sf_applications sfa
  ON c.applicationfilefilenumber = sfa.application_number
JOIN delta.sf_loans sfl
  ON c.loanid = sfl.loan_id
 AND sfa.opportunity_id = sfl.application_id
JOIN fc05_records fc05
  ON c.applicationfilefilenumber = fc05.application_number
 AND sfl.macao_loan_id = fc05.loan_id
WHERE c.id IS NOT NULL
  AND fc05.application_number IN (
      SELECT application_number
      FROM fc05_records
      GROUP BY application_number
      HAVING COUNT(*) = 1
         AND MAX(cover_type) = '090'
  );