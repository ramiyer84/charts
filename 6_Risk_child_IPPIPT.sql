SELECT
    salesforce_loan_i AS "salesforce_loan_id",
    application_numbe AS "application_number(Name)",

    -- Dynamic pricing name
    'RiskChild' || ROW_NUMBER() OVER (PARTITION BY application_numbe ORDER BY salesforce_loan_i) AS "Pricing name",

    -- Static values
    '1' AS "Version(TM_B02_Version__c)",
    'TRUE' AS "isActiveVersion(TM_B02_isActiveVersion__c)",
    '1' AS "Current Tab Level(TM_B02_CurrentTabLevel__c)",
    'Medical' AS "Risk Type(TM_B02_Risk_Type__c)",
    'IPPIPT' AS "Cover Group Type(TM_B02_Cover_Group_Type__c)",
    'Group' AS "Risk cover pricing type(TM_B02_Risk_CoverPricing_Type__c)",

    overpremium_invalidity_ra AS "overpremium_invalidity_rate(TM_B02_Cover_Group_ExtraPremium_Rate__c)",
    '0' AS "Cover group total rate(TM_B02_Cover_Group_Total_Rate__c)",

    '012G500000z06kDkd' AS "Recordtype",
    NULL AS "Risk cover details(Related to riskrate)(TM_B02_Risk_CoverDetails__c)"

FROM
    pricing_original
WHERE
    salesforce_loan_i IS NOT NULL
ORDER BY
    application_numbe,
    salesforce_loan_i;