WITH one_to_one_mapping AS (
  SELECT '050' AS macao_cover_type, '10' AS expected_mrt_enum
  UNION ALL SELECT '060', '2'
  UNION ALL SELECT '310', '0'
),
macao_covers AS (
  SELECT 
    fc05.application_number,
    fc05.loan_id AS macao_technical_loan_number,
    fc05.cover_number,
    fc05.cover_type AS macao_cover_type,
    fc05.cover_label AS macao_cover_label,
    fc05.created_at AS macao_creation_date,
    fc05.insured_amount AS macao_insured_amount_by_cover,
    fc05.submitted AS macao_cover_submitted_to_medical_selection,
    fc05.currency_code AS macao_currency_code,
    fc05.optional_cover_code AS optional_cover_code,
    fc02.loan_number AS macao_loan_number,
    fc02.rpp AS macao_rpp,
    fc02.insured_capita_rate AS macao_quota,
    fc02.aeras_capping AS macao_capping_indicator,
    mrt.loan_id AS mrt_loan_id,
    mrt.application_id AS mrt_application_id,
    CASE WHEN mrt.loan_id IS NULL THEN TRUE ELSE FALSE END AS is_mrt_loan_missing
  FROM fc05_records fc05
  INNER JOIN fc02_records fc02 
    ON fc05.loan_id = fc02.loan_id 
   AND fc05.application_number = fc02.application_number
  LEFT JOIN delta.sf_loans mrt 
    ON TRIM(fc05.loan_id::text) = TRIM(mrt.macao_loan_id::text)
  WHERE fc05.cover_type IN ('050', '060', '310')
),
macao_with_enum AS (
  SELECT mc.*, map.expected_mrt_enum
  FROM macao_covers mc
  JOIN one_to_one_mapping map 
    ON mc.macao_cover_type = map.macao_cover_type
)

SELECT 
  mc.application_number,
  mc.macao_technical_loan_number,
  mc.cover_number,
  mc.macao_cover_type,
  mc.expected_mrt_enum,
  mc.macao_cover_label,
  mc.macao_creation_date,
  mc.macao_insured_amount_by_cover,
  mc.macao_cover_submitted_to_medical_selection,
  mc.macao_currency_code,
  mc.optional_cover_code,
  mc.macao_loan_number,
  mc.macao_rpp,
  mc.macao_quota,
  mc.macao_capping_indicator,
  mc.mrt_loan_id,
  mc.mrt_application_id,
  mc.is_mrt_loan_missing
FROM macao_with_enum mc
WHERE NOT EXISTS (
    SELECT 1
    FROM delta.dms_covers d
    WHERE TRIM(d.loan_id::text) = TRIM(mc.mrt_loan_id::text)
      AND TRIM(d.macao_cover_id::text) = TRIM(mc.cover_number::text)
      AND d.cover_type = mc.expected_mrt_enum
)
ORDER BY mc.application_number, mc.cover_number;