SELECT
    f.application_number,
    f.loan_id               AS fc02_loan_id,
    f.loan_number           AS fc02_loan_number,
    f.rpp                   AS fc02_rpp,
    f.loan_capital_amount   AS fc02_capital_amount,
    f.loan_duration_months  AS fc02_duration_months,
    s.loan_id               AS sf_loan_id,
    s.macao_loan_id,
    s.loan_number           AS sf_loan_number,
    s.loan_duration         AS sf_duration,
    s.loan_amount           AS sf_loan_amount,
    s.scheme_name           AS sf_scheme_name,

    CASE
        WHEN s.loan_id IS NULL THEN FALSE
        WHEN f.loan_capital_amount = s.loan_amount
         AND f.loan_duration_months = s.loan_duration
         AND f.rpp = s.scheme_name THEN TRUE
        ELSE FALSE
    END AS is_matched,

    CASE
        WHEN f.application_number < '2021A024046' THEN 'P1 matching'
        WHEN s.loan_id IS NULL THEN 'No loan in MRT'
        WHEN f.loan_capital_amount <> s.loan_amount
             AND f.loan_duration_months <> s.loan_duration THEN 'Loan amount & duration mismatch'
        WHEN f.loan_capital_amount <> s.loan_amount THEN 'Loan amount mismatch'
        WHEN f.loan_duration_months <> s.loan_duration THEN 'Loan duration mismatch'
        WHEN f.rpp <> s.scheme_name THEN 'Scheme mismatch'
        ELSE 'All match'
    END AS matching_comments

FROM fc02_records f
LEFT JOIN delta.sf_loans s
    ON s.macao_loan_id = f.loan_id
   AND s.loan_number = f.loan_number
-- optionally also match by application number if needed
--   AND s.application_number = f.application_number
WHERE f.application_number >= '2021A024046'  -- filter out Phase 1 from the main result
ORDER BY f.application_number, f.loan_id;