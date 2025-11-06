-- One row per (application_number, loan_id) that is PE-only
SELECT
  application_number,
  loan_id,
  MAX(cover_number) AS cover_number,   -- safe because there’s exactly 1 row
  MAX(cover_type)   AS cover_type
FROM fc05_records
GROUP BY application_number, loan_id
HAVING COUNT(*) = 1
   AND MAX(cover_type) = '090';





WITH loan_pe_only AS (
  SELECT application_number, loan_id
  FROM fc05_records
  GROUP BY application_number, loan_id
  HAVING COUNT(*) = 1 AND MAX(cover_type) = '090'
),
loan_counts AS (
  SELECT application_number, COUNT(DISTINCT loan_id) AS total_loans
  FROM fc05_records
  GROUP BY application_number
),
pe_counts AS (
  SELECT application_number, COUNT(*) AS pe_loans
  FROM loan_pe_only
  GROUP BY application_number
)
SELECT lc.application_number
FROM loan_counts lc
JOIN pe_counts  pc ON pc.application_number = lc.application_number
WHERE lc.total_loans = pc.pe_loans;   -- all loans are PE-only






SELECT DISTINCT application_number
FROM fc05_records
GROUP BY application_number, loan_id
HAVING COUNT(*) = 1 AND MAX(cover_type) = '090';




SELECT application_number, loan_id, cover_type, COUNT(*) AS cnt
FROM fc05_records
WHERE application_number IN ('2001A087243','2001A026848')
GROUP BY application_number, loan_id, cover_type
ORDER BY application_number, loan_id;