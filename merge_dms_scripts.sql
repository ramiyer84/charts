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
   */

MERGE sch_DMSDECISIONS.Decisions AS tgt
USING (
    SELECT 
        src.*,
        CASE 
            WHEN src.AdjournmentId IS NOT NULL 
                 AND EXISTS (SELECT 1 FROM sch_DMSDECISIONS.Adjournments a WHERE a.Id = src.AdjournmentId)
            THEN src.AdjournmentId 
            ELSE NULL 
        END AS ValidAdjournmentId,

        CASE 
            WHEN src.OptionId IS NOT NULL
                 AND EXISTS (SELECT 1 FROM sch_DMSDECISIONS.Options o WHERE o.Id = src.OptionId)
            THEN src.OptionId
            ELSE NULL
        END AS ValidOptionId
    FROM sch_DMSDECISIONS.temp_DecisionsCsv src
) AS vsrc
ON tgt.Id = vsrc.Id
WHEN MATCHED THEN
    UPDATE SET
        tgt.RiskType                = vsrc.RiskType,
        tgt.DecisionStatus          = vsrc.DecisionStatus,
        tgt.HasAmountLimit          = vsrc.HasAmountLimit,
        tgt.HasContractualExclusion = vsrc.HasContractualExclusion,
        tgt.HasExtrapremium         = vsrc.HasExtrapremium,
        tgt.HasPartialExclusion     = vsrc.HasPartialExclusion,
        tgt.HasWarrantyLimit        = vsrc.HasWarrantyLimit,
        tgt.LoanId                  = vsrc.LoanId,
        tgt.CoverageId              = vsrc.CoverageId,
        tgt.OptionId                = vsrc.ValidOptionId,        -- NEW
        tgt.AmountLimit             = vsrc.AmountLimit,
        tgt.DecisionType            = vsrc.DecisionType,
        tgt.WarrantyLimit           = vsrc.WarrantyLimit,
        tgt.AdjournmentId           = vsrc.ValidAdjournmentId,   -- NEW
        tgt.AerasLevel              = vsrc.AerasLevel,
        tgt.IsActiveTabLevel        = vsrc.IsActiveTabLevel,
        tgt.TabLevel                = vsrc.TabLevel,
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
        OptionId,
        AmountLimit,
        DecisionType,
        WarrantyLimit,
        AdjournmentId,
        AerasLevel,
        IsActiveTabLevel,
        TabLevel
    )
    VALUES (
        COALESCE(vsrc.Id, NEWID()),
        SYSDATETIME(),
        SYSDATETIME(),
        vsrc.RiskType,
        vsrc.DecisionStatus,
        vsrc.HasAmountLimit,
        vsrc.HasContractualExclusion,
        vsrc.HasExtrapremium,
        vsrc.HasPartialExclusion,
        vsrc.HasWarrantyLimit,
        vsrc.LoanId,
        vsrc.CoverageId,
        vsrc.ValidOptionId,        -- NEW
        vsrc.AmountLimit,
        vsrc.DecisionType,
        vsrc.WarrantyLimit,
        vsrc.ValidAdjournmentId,   -- NEW
        vsrc.AerasLevel,
        vsrc.IsActiveTabLevel,
        vsrc.TabLevel
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