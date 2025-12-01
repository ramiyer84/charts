/* ============================================================
   STEP 0 – Clear yesterday’s staging data
   (Run this BEFORE importing today's CSVs)
   ============================================================ */
TRUNCATE TABLE sch_DMSDECISIONS.temp_DecisionsCsv;
TRUNCATE TABLE sch_DMSDECISIONS.temp_CoverageExclusionsCsv;
TRUNCATE TABLE sch_DMSDECISIONS.temp_CoveragePremiumsCsv;

/*
    >>> NOW, IN INTELLIJ:
        - Import Decisions.csv into sch_DMSDECISIONS.temp_DecisionsCsv
        - Import CoverageExclusions.csv into sch_DMSDECISIONS.temp_CoverageExclusionsCsv
        - Import CoveragePremiums.csv into sch_DMSDECISIONS.temp_CoveragePremiumsCsv
*/


/* ============================================================
   STEP 1 – MERGE Decisions
   - Updates existing Decisions by Id
   - Inserts new Decisions where Id not present
   - DOES NOT touch OptionId or AdjournmentId (avoids FK issues)
   ============================================================ */
MERGE sch_DMSDECISIONS.Decisions AS tgt
USING sch_DMSDECISIONS.temp_DecisionsCsv AS src
      ON tgt.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET
        tgt.RiskType                = src.RiskType,
        tgt.DecisionStatus          = src.DecisionStatus,
        tgt.HasAmountLimit          = src.HasAmountLimit,
        tgt.HasContractualExclusion = src.HasContractualExclusion,
        tgt.HasExtrapremium         = src.HasExtrapremium,
        tgt.HasPartialExclusion     = src.HasPartialExclusion,
        tgt.HasWarrantyLimit        = src.HasWarrantyLimit,
        tgt.LoanId                  = src.LoanId,
        tgt.CoverageId              = src.CoverageId,
        -- tgt.OptionId             = src.OptionId,        -- intentionally not updated
        tgt.AmountLimit             = src.AmountLimit,
        tgt.DecisionType            = src.DecisionType,
        tgt.WarrantyLimit           = src.WarrantyLimit,
        -- tgt.AdjournmentId        = src.AdjournmentId,   -- intentionally not updated
        tgt.AerasLevel              = src.AerasLevel,
        tgt.IsActiveTabLevel        = src.IsActiveTabLevel,
        tgt.TabLevel                = src.TabLevel,
        tgt.ModificationDate        = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        Id,
        CreationDate,
        ModificationDate,
        RiskType,
        DecisionStatus,
        HasAmountLimit,
        HasContractualExclusion,
        HasExtrapremium,
        HasPartialExclusion,
        HasWarrantyLimit,
        LoanId,
        CoverageId,
        -- OptionId,                -- intentionally not inserted
        AmountLimit,
        DecisionType,
        WarrantyLimit,
        -- AdjournmentId,           -- intentionally not inserted
        AerasLevel,
        IsActiveTabLevel,
        TabLevel
    )
    VALUES (
        COALESCE(src.Id, NEWID()),
        SYSDATETIME(),
        SYSDATETIME(),
        src.RiskType,
        src.DecisionStatus,
        src.HasAmountLimit,
        src.HasContractualExclusion,
        src.HasExtrapremium,
        src.HasPartialExclusion,
        src.HasWarrantyLimit,
        src.LoanId,
        src.CoverageId,
        -- NULL,
        src.AmountLimit,
        src.DecisionType,
        src.WarrantyLimit,
        -- NULL,
        src.AerasLevel,
        src.IsActiveTabLevel,
        src.TabLevel
    );
GO


/* ============================================================
   STEP 2 – MERGE CoverageExclusions
   - Inserts / updates rows based on Id
   - Generates Id when missing using NEWID()
   ============================================================ */
MERGE sch_DMSDECISIONS.CoverageExclusions AS tgt
USING sch_DMSDECISIONS.temp_CoverageExclusionsCsv AS src
      ON tgt.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET
        tgt.ExclusionType    = src.ExclusionType,
        tgt.Code             = src.Code,
        tgt.Description      = src.Description,
        tgt.DecisionId       = src.DecisionId,
        tgt.IsArchived       = src.IsArchived,
        tgt.AuthorId         = src.AuthorId,
        tgt.RowIndex         = src.RowIndex,
        tgt.ModificationDate = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        Id,
        CreationDate,
        ModificationDate,
        ExclusionType,
        Code,
        Description,
        DecisionId,
        IsArchived,
        AuthorId,
        RowIndex
    )
    VALUES (
        COALESCE(src.Id, NEWID()),
        SYSDATETIME(),
        SYSDATETIME(),
        src.ExclusionType,
        src.Code,
        src.Description,
        src.DecisionId,
        src.IsArchived,
        src.AuthorId,
        src.RowIndex
    );
GO


/* ============================================================
   STEP 3 – MERGE CoveragePremiums
   - Inserts / updates rows based on Id
   - Generates Id when missing using NEWID()
   ============================================================ */
MERGE sch_DMSDECISIONS.CoveragePremiums AS tgt
USING sch_DMSDECISIONS.temp_CoveragePremiumsCsv AS src
      ON tgt.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET
        tgt.LinkType         = src.LinkType,
        tgt.PremiumType      = src.PremiumType,
        tgt.Rate             = src.Rate,
        tgt.Unit             = src.Unit,
        tgt.Duration         = src.Duration,
        tgt.DecisionId       = src.DecisionId,
        tgt.IsArchived       = src.IsArchived,
        tgt.AuthorId         = src.AuthorId,
        tgt.RowIndex         = src.RowIndex,
        tgt.ModificationDate = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        Id,
        CreationDate,
        ModificationDate,
        LinkType,
        PremiumType,
        Rate,
        Unit,
        Duration,
        DecisionId,
        IsArchived,
        AuthorId,
        RowIndex
    )
    VALUES (
        COALESCE(src.Id, NEWID()),
        SYSDATETIME(),
        SYSDATETIME(),
        src.LinkType,
        src.PremiumType,
        src.Rate,
        src.Unit,
        src.Duration,
        src.DecisionId,
        src.IsArchived,
        src.AuthorId,
        src.RowIndex
    );
GO


/* ============================================================
   STEP 4 – Quick sanity check (optional)
   Recently touched rows in last 5 minutes
   ============================================================ */
SELECT COUNT(*) AS Decisions_Touched_5min
FROM sch_DMSDECISIONS.Decisions
WHERE ModificationDate > DATEADD(MINUTE, -5, SYSDATETIME())
   OR CreationDate     > DATEADD(MINUTE, -5, SYSDATETIME());

SELECT COUNT(*) AS Exclusions_Touched_5min
FROM sch_DMSDECISIONS.CoverageExclusions
WHERE ModificationDate > DATEADD(MINUTE, -5, SYSDATETIME())
   OR CreationDate     > DATEADD(MINUTE, -5, SYSDATETIME());

SELECT COUNT(*) AS Premiums_Touched_5min
FROM sch_DMSDECISIONS.CoveragePremiums
WHERE ModificationDate > DATEADD(MINUTE, -5, SYSDATETIME())
   OR CreationDate     > DATEADD(MINUTE, -5, SYSDATETIME());