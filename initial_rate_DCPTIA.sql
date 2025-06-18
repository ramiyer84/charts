SELECT
    salesforce_loan_i AS "salesforce_loan_id",
    application_numbe AS "application_number(Name)",

    -- Dynamic name: InitialRate1, InitialRate2...
    'InitialRate' || ROW_NUMBER() OVER (PARTITION BY application_numbe ORDER BY salesforce_loan_i) AS "Pricing Rates Name(Name)",

    -- Static values
    '1' AS "Version(TM_B02_Version__c)",
    'TRUE' AS "isActiveVersion(TM_B02_isActiveVersion__c)",
    '1' AS "Current Tab Level(TM_B02_CurrentTabLevel__c)",
    'DCPTIA' AS "Cover Group Type(TM_B02_Cover_Group_Type__c)",
    '012G500000z06kDkd' AS "RecordTypeID",
    NULL AS "PricingRelatedToParent(TM_B02_Initial__c)",

    -- Extracted and static data
    normal_death_rat AS "Cover Group Initial Rate(TM_B02_Cover_Group_Initial_Rate__c)",
    '0' AS "TM_B02_Cover_Group_Total_Rate__c"

FROM
    pricing_original
WHERE
    salesforce_loan_i IS NOT NULL
ORDER BY
    application_numbe,
    salesforce_loan_i;