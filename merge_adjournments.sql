/* =====================================================================
   ONE-TIME SETUP
   Create staging table for Adjournments CSV import
   ===================================================================== */
CREATE TABLE sch_DMSDECISIONS.temp_AdjournmentsCsv (
    Id UNIQUEIDENTIFIER NULL,
    CreationDate NVARCHAR(50) NULL,        -- CSV value, ignored
    ModificationDate NVARCHAR(50) NULL,    -- CSV value, ignored
    AdjournmentType INT NULL,
    AdjournmentDetail NVARCHAR(510) NULL,
    IsArchived BIT NULL
);
GO


/* =====================================================================
   DAILY PART — RUN THIS EACH DAY
   STEP 1: Clear staging table BEFORE importing today's CSV
   ===================================================================== */
TRUNCATE TABLE sch_DMSDECISIONS.temp_AdjournmentsCsv;
GO

/* =====================================================================
   >>> NOW IMPORT Adjournments.csv using IntelliJ:
       - Right-click sch_DMSDECISIONS.temp_AdjournmentsCsv
         → "Import Data from File…"
       - Map columns: Id, AdjournmentType, AdjournmentDetail, IsArchived, etc.
       - Load the CSV into temp_AdjournmentsCsv
   ===================================================================== */


/* =====================================================================
   DAILY PART — RUN AFTER CSV IMPORT
   STEP 2: Upsert Adjournments using MERGE
   - Updates existing Adjournments by Id
   - Inserts new rows when Id not found (or blank in CSV)
   - Uses SYSDATETIME() for CreationDate / ModificationDate
   ===================================================================== */
MERGE sch_DMSDECISIONS.Adjournments AS tgt
USING sch_DMSDECISIONS.temp_AdjournmentsCsv AS src
      ON tgt.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET
        tgt.AdjournmentType   = src.AdjournmentType,
        tgt.AdjournmentDetail = src.AdjournmentDetail,
        tgt.IsArchived        = src.IsArchived,
        tgt.ModificationDate  = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        Id,
        CreationDate,
        ModificationDate,
        AdjournmentType,
        AdjournmentDetail,
        IsArchived
    )
    VALUES (
        COALESCE(src.Id, NEWID()),   -- generate Id if CSV has blank
        SYSDATETIME(),               -- CreationDate
        SYSDATETIME(),               -- ModificationDate
        src.AdjournmentType,
        src.AdjournmentDetail,
        src.IsArchived
    );
GO


/* =====================================================================
   OPTIONAL — QUICK VALIDATION: recently touched Adjournments
   ===================================================================== */
SELECT TOP 20 *
FROM sch_DMSDECISIONS.Adjournments
WHERE ModificationDate > DATEADD(MINUTE, -5, SYSDATETIME())
   OR CreationDate     > DATEADD(MINUTE, -5, SYSDATETIME())
ORDER BY ModificationDate DESC;
GO