/* =====================================================================
   ONE-TIME SETUP  
   Create staging table for Notes CSV import
   ===================================================================== */
CREATE TABLE sch_DMSDECISIONS.temp_NotesCsv (
    Id UNIQUEIDENTIFIER NULL,
    CreationDate NVARCHAR(50) NULL,        -- CSV value, ignored
    ModificationDate NVARCHAR(50) NULL,    -- CSV value, ignored
    Reason NVARCHAR(510) NULL,
    AuthorId NVARCHAR(MAX) NULL,
    ApplicationFileFileNumber NVARCHAR(450) NULL,
    AerasLevel INT NULL,
    TabLevel INT NULL,
    IsArchived BIT NULL
);
GO


/* =====================================================================
   DAILY PART — RUN THIS EACH DAY
   Step 1: Clear staging table BEFORE importing today's CSV
   ===================================================================== */
TRUNCATE TABLE sch_DMSDECISIONS.temp_NotesCsv;
GO

/* =====================================================================
   >>> NOW IMPORT Notes.csv using IntelliJ:
       - Right-click temp_NotesCsv → Import Data from File…
       - Map Id → Id, Reason → Reason, etc.
       - Load the CSV into temp_NotesCsv
   ===================================================================== */


/* =====================================================================
   DAILY PART — RUN AFTER CSV IMPORT
   Step 2: Upsert Notes using MERGE
   ===================================================================== */
MERGE sch_DMSDECISIONS.Notes AS tgt
USING sch_DMSDECISIONS.temp_NotesCsv AS src
      ON tgt.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET
        tgt.Reason                    = src.Reason,
        tgt.AuthorId                  = src.AuthorId,
        tgt.ApplicationFileFileNumber = src.ApplicationFileFileNumber,
        tgt.AerasLevel                = src.AerasLevel,
        tgt.TabLevel                  = src.TabLevel,
        tgt.IsArchived                = src.IsArchived,
        tgt.ModificationDate          = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        Id,
        CreationDate,
        ModificationDate,
        Reason,
        AuthorId,
        ApplicationFileFileNumber,
        AerasLevel,
        TabLevel,
        IsArchived
    )
    VALUES (
        COALESCE(src.Id, NEWID()),   -- generate new Id if CSV has blank
        SYSDATETIME(),               -- CreationDate
        SYSDATETIME(),               -- ModificationDate
        src.Reason,
        src.AuthorId,
        src.ApplicationFileFileNumber,
        src.AerasLevel,
        src.TabLevel,
        src.IsArchived
    );
GO


/* =====================================================================
   OPTIONAL — QUICK VALIDATION: See recently touched Notes
   ===================================================================== */
SELECT TOP 20 *
FROM sch_DMSDECISIONS.Notes
WHERE ModificationDate > DATEADD(MINUTE, -5, SYSDATETIME())
   OR CreationDate     > DATEADD(MINUTE, -5, SYSDATETIME())
ORDER BY ModificationDate DESC;
GO