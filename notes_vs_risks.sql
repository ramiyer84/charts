SELECT 
    Description,
    AuthorId,
    NoteType,
    COUNT(DISTINCT RiskId) AS RiskIdCount,
    COUNT(*) AS TotalDuplicates
FROM Notes
GROUP BY 
    Description,
    AuthorId,
    NoteType
HAVING COUNT(DISTINCT RiskId) > 1;


WITH DuplicateGroups AS (
    SELECT 
        Description,
        AuthorId,
        NoteType
    FROM Notes
    GROUP BY 
        Description,
        AuthorId,
        NoteType
    HAVING COUNT(DISTINCT RiskId) > 1
)
SELECT n.*
FROM Notes n
JOIN DuplicateGroups dg
    ON n.Description = dg.Description
    AND n.AuthorId = dg.AuthorId
    AND n.NoteType = dg.NoteType
ORDER BY n.Description, n.AuthorId, n.NoteType, n.RiskId;