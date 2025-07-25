SELECT
    salesforce_loan_i AS "salesforce_loan_id",
    application_numbe AS "application_number(Name)",

    -- Dynamic name per loan in application
    'PricingRiskChild' || ROW_NUMBER() OVER (PARTITION BY application_numbe ORDER BY salesforce_loan_i) AS "Pricing Rate name",

    -- Static mappings
    '1' AS "Version(TM_B02_Version__c)",
    'TRUE' AS "isActiveVersion(TM_B02_isActiveVersion__c)",
    '1' AS "Current Tab Level(TM_B02_CurrentTabLevel__c)",
    'Medical' AS "Risk Type(TM_B02_Risk_Type__c)",
    'DCPTIA' AS "Cover Group Type(TM_B02_Cover_Group_Type__c)",
    'Group' AS "Risk cover pricing type(TM_B02_Risk_CoverPricing_Type__c)",
    
    overpremium_death_rat AS "overpremium_death_rate(TM_B02_Cover_Group_ExtraPremium_Rate__c)",
    '0' AS "Cover group total(TM_B02_Cover_Group_Total_Rate__c)",

    '012G500000z06kDkd' AS "Recordtype",
    NULL AS "Risk cover details(Related to riskrate)(TM_B02_Risk_CoverDetail__c)"

FROM
    pricing_original
WHERE
    salesforce_loan_i IS NOT NULL
ORDER BY
    application_numbe,
    salesforce_loan_i;