-- Decisions

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
        tgt.OptionId                = src.OptionId,
        tgt.AmountLimit             = src.AmountLimit,
        tgt.DecisionType            = src.DecisionType,
        tgt.WarrantyLimit           = src.WarrantyLimit,
        tgt.AdjournmentId           = src.AdjournmentId,
        tgt.AerasLevel              = src.AerasLevel,
        tgt.IsActiveTabLevel        = src.IsActiveTabLevel,
        tgt.TabLevel                = src.TabLevel,
        tgt.ModificationDate        = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        Id,              -- ✅ now we explicitly insert Id
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
        COALESCE(src.Id, NEWID()),   -- ✅ keep Id from CSV if present, else generate one
        SYSDATETIME(),               -- CreationDate
        SYSDATETIME(),               -- ModificationDate
        src.RiskType,
        src.DecisionStatus,
        src.HasAmountLimit,
        src.HasContractualExclusion,
        src.HasExtrapremium,
        src.HasPartialExclusion,
        src.HasWarrantyLimit,
        src.LoanId,
        src.CoverageId,
        src.OptionId,
        src.AmountLimit,
        src.DecisionType,
        src.WarrantyLimit,
        src.AdjournmentId,
        src.AerasLevel,
        src.IsActiveTabLevel,
        src.TabLevel
    );


-- Coverage Exclusions

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
        Id,              -- ✅ explicit
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
        COALESCE(src.Id, NEWID()),   -- all your CSV Ids are blank → generates new GUIDs
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


-- Coverage Premiums

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
        Id,              -- ✅ explicit
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