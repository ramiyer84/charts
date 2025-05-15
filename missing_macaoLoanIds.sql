WITH one_to_one_mapping AS (
  SELECT '050' AS macao_cover_type, '10' AS expected_mrt_enum
  UNION ALL SELECT '060', '2'
  UNION ALL SELECT '310', '0'
),
macao_with_expected_enum AS (
  SELECT 
    fc05.application_number,
    fc05.loan_id AS macao_loan_id,
    fc05.cover_number,
    fc05.cover_type AS macao_cover_type,
    map.expected_mrt_enum,
    mrt.loan_id AS mrt_loan_id,
    mrt.application_id AS mrt_application_id
  FROM fc05_records fc05
  INNER JOIN one_to_one_mapping map 
    ON fc05.cover_type = map.macao_cover_type
  LEFT JOIN delta.sf_loans mrt 
    ON TRIM(fc05.loan_id::text) = TRIM(mrt.macao_loan_id::text)
  WHERE fc05.cover_type IN ('050', '060', '310')
),
mrt_covers_with_null_ids AS (
  SELECT 
    m.application_number,
    m.macao_loan_id,
    m.cover_number,
    m.macao_cover_type,
    m.expected_mrt_enum,
    m.mrt_loan_id,
    m.mrt_application_id,
    d.cover_id,
    d.cover_type AS mrt_cover_type,
    d.macao_cover_id,
    CASE 
      WHEN d.cover_id IS NOT NULL AND d.macao_cover_id IS NULL THEN 'Cover exists in MRT but macao_cover_id is missing'
      ELSE 'Not applicable'
    END AS issue_reason
  FROM macao_with_expected_enum m
  LEFT JOIN delta.dms_covers d
    ON TRIM(d.loan_id::text) = TRIM(m.mrt_loan_id::text)
   AND d.cover_type::text = m.expected_mrt_enum
)

SELECT *
FROM mrt_covers_with_null_ids
WHERE issue_reason = 'Cover exists in MRT but macao_cover_id is missing'
ORDER BY application_number, cover_number;