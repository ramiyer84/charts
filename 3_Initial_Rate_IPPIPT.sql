SELECT
    salesforce_loan_i AS "salesforce_loan_id",
    application_numbe AS "application_number(Name)",

    -- Dynamic naming: PricingInitialRate1, PricingInitialRate2...
    'PricingInitialRate' || ROW_NUMBER() OVER (PARTITION BY application_numbe ORDER BY salesforce_loan_i) AS "Pricing Rates Name(Name)",

    -- Static fields
    '1' AS "Version(TM_B02_Version__c)",
    'TRUE' AS "isActiveVersion(TM_B02_isActiveVersion__c)",
    '1' AS "Current Tab Level(TM_B02_CurrentTabLevel__c)",
    'IPPIPT' AS "Cover Group Type(TM_B02_Cover_Group_Type__c)",
    '012G500000z06kDkd' AS "RecordTypeID",
    NULL AS "PricingRelatedToParent(TM_B02_Initial__c)",

    -- Rate values
    normal_invalidity_ra AS "normal_invalidity_rate(TM_B02_Cover_Group_Initial_Rate__c)",
    '0' AS "TM_B02_Cover_Group_Total_Rate__c"

FROM
    pricing_original
WHERE
    salesforce_loan_i IS NOT NULL
ORDER BY
    application_numbe,
    salesforce_loan_i;