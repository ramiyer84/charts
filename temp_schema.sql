-- Decisions.csv
CREATE TABLE sch_DMSDECISIONS.temp_DecisionsCsv (
    Id UNIQUEIDENTIFIER NULL,
    CreationDate NVARCHAR(50) NULL,
    ModificationDate NVARCHAR(50) NULL,
    RiskType INT,
    DecisionStatus INT,
    HasAmountLimit BIT,
    HasContractualExclusion BIT,
    HasExtrapremium BIT,
    HasPartialExclusion BIT,
    HasWarrantyLimit BIT,
    LoanId NVARCHAR(36),
    CoverageId UNIQUEIDENTIFIER NULL,
    OptionId UNIQUEIDENTIFIER NULL,
    AmountLimit NVARCHAR(510) NULL,
    DecisionType INT,
    WarrantyLimit NVARCHAR(510) NULL,
    AdjournmentId UNIQUEIDENTIFIER NULL,
    AerasLevel INT,
    IsActiveTabLevel BIT,
    TabLevel INT
);

-- CoverageExclusions.csv
CREATE TABLE sch_DMSDECISIONS.temp_CoverageExclusionsCsv (
    Id UNIQUEIDENTIFIER NULL,
    CreationDate NVARCHAR(50) NULL,
    ModificationDate NVARCHAR(50) NULL,
    ExclusionType INT,
    Code NVARCHAR(20),
    Description NVARCHAR(510),
    DecisionId UNIQUEIDENTIFIER,
    IsArchived BIT,
    AuthorId NVARCHAR(200),
    RowIndex INT
);

-- CoveragePremiums.csv
CREATE TABLE sch_DMSDECISIONS.temp_CoveragePremiumsCsv (
    Id UNIQUEIDENTIFIER NULL,
    CreationDate NVARCHAR(50) NULL,
    ModificationDate NVARCHAR(50) NULL,
    LinkType INT,
    PremiumType INT,
    Rate INT,
    Unit INT,
    Duration INT,
    DecisionId UNIQUEIDENTIFIER,
    IsArchived BIT,
    AuthorId NVARCHAR(200),
    RowIndex INT
);