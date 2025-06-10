SELECT
  f02.*
FROM
  fc02_records f02
LEFT JOIN delta.sf_applications sa
  ON f02.application_number = sa.application_number
LEFT JOIN delta.sf_loans st
  ON sa.opportunity_id = st.application_id
WHERE
  f02.gis_areas_cover IN ('1', '2', 'R')
  AND st.loan_id = TRIM(f02.loan_id)
  AND NOT EXISTS (
    SELECT 1
    FROM dms.dms_coverages c
    WHERE
      TRIM(c.loanid) = st.loan_id
      AND TRIM(c.applicationfilenumber) = f02.application_number
      -- Add any other relevant conditions from your original query
  )
  AND EXISTS (
    SELECT 1
    FROM fc01_records fc01
    WHERE
      fc01.application_number = TRIM(f02.application_number)
      -- Uncomment and adjust the next line if needed:
      -- AND fc01.applicationfilenumber = '2021A023130'
  )
