SELECT
    salesforce_loan_i AS "salesforce_loan_id",
    application_numbe AS "application_number(Name)",

    -- Dynamic pricing name
    'PricingRiskRate' || ROW_NUMBER() OVER (PARTITION BY application_numbe ORDER BY salesforce_loan_i) AS "Pricing Rate name",

    -- Static values
    '1' AS "Version(TM_B02_Version__c)",
    'TRUE' AS "isActiveVersion(TM_B02_isActiveVersion__c)",
    '1' AS "Current Tab Level(TM_B02_CurrentTabLevel__c)",
    'Medical' AS "Risk Type(TM_B02_Risk_Type__c)",
    '012G500000z06kDkd' AS "RecordTypeID",
    NULL AS "PricingRelatedToParent(TM_B02_Initial__c)",

    -- Risk-specific data
    overpremium_rat AS "overpremium_rate(TM_B02_RiskTabExtraPremiumRate__c)",
    '0' AS "Risk Total Rate(TM_B02_Risk_Total_Rate__c)"

FROM
    pricing_original
WHERE
    salesforce_loan_i IS NOT NULL
ORDER BY
    application_numbe,
    salesforce_loan_i;