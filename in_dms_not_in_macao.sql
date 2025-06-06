SELECT 
  c.id,
  c.creationdate,
  c.modificationdate,
  c.loanid,
  c.name,
  c.quota,
  c.applicationfilefilenumber,
  c.aerascappingapplies,
  c.coversubmitted,
  c.currencycode,
  c.insuredamount,
  c.macaocoverid
FROM coverages c
WHERE c.name = '5'
  AND NOT EXISTS (
    SELECT 1
    FROM fc02_records f02
    LEFT JOIN delta.sf_applications sa
      ON f02.application_number = sa.application_number
    LEFT JOIN delta.sf_loans sl
      ON sa.opportunity_id = sl.application_id
     AND f02.loan_id = sl.macao_loan_id
    WHERE f02.gis_aeras_cover NOT IN ('1','2','R')
      AND sl.loan_id = c.loanid
  )