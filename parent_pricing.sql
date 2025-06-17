SELECT
    salesforce_loan_i AS "salesforce_loan_id",
    application_numbe AS "application_number(Name)",

    -- Dynamic Pricing Rates Name: Pricing 1, Pricing 2, etc.
    'Pricing ' || ROW_NUMBER() OVER (PARTITION BY application_numbe ORDER BY salesforce_loan_i) AS "Pricing Rates Name(Name)",

    -- Static fields and mappings
    '01265000000z06ke' AS "Record Type Id",
    '1' AS "Version(TM_B02_Version__c)",
    'TRUE' AS "isActiveVersion(TM_B02_isActiveVersion__c)",
    '1' AS "Current Tab Level(TM_B02_CurrentTabLevel__c)",

    premium_called_rat AS "premium_called_rate(TM_B02_Premium_Rate__c)",
    normal_rate AS "normal_rate(TM_B02_Initial_Premium_Rate__c)",
    monthly_premium_amou AS "monthly_premium_amount(TM_B02_Monthly_Premium__c)",

    (monthly_premium_amou * 3) AS "TP_Premium(TM_B02_Premium__c)",

    'Monthly' AS "Billing Frequency(TM_B02_Billing_Frequency__c)",
    DATE '2025-04-13' AS "Pricing creation date(TM_B02_Pricing_Creation_Date__c)"

FROM
    pricing_original
WHERE
    salesforce_loan_i IS NOT NULL
ORDER BY
    application_numbe,
    salesforce_loan_i;