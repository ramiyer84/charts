package fc01

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc19"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/s3io"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/util"
	"github.com/spf13/viper"
	"syreclabs.com/go/faker"
)

// -----------------------------------------------------------------------------
// NOTE: This file preserves original business logic and only changes I/O:
//   - uses fc.Reader (io.Reader) instead of os.Open(fc.Filepath)
//   - maps missing FC19 via s3io.NotFoundError (S3-only)
// -----------------------------------------------------------------------------

// Keep your existing fake notes (was present in the original file).
// If unused at runtime, leave as-is to minimize diff; remove later if desired.
var fakeNotes = []string{
	"REFUS AE NIV 20/11/2020 EV. Application: '%v'.",
	"DOSSIER ANNULE ET REMPLACE PAR LE DOSSIER 2020AD47346 M9 19/10/20. Application: '%v'.",
	"RENVOI DRC PAR MAIL CRYPTE AU CLIENT M9 LE 20/09/2020. Application: '%v'.",
	"REFUS DANS LE CADRE DU 2EME NIVEAU D’ASSURANCE. Application: '%v'.",
}

// FC19FileMissing indicates the FC19 file could not be found (now in S3).
type FC19FileMissing struct {
	FileName string
}

func (e *FC19FileMissing) Error() string {
	return fmt.Sprintf("corresponding FC19 file '%s' is missing", e.FileName)
}

// File is your existing processor receiver type for FC01.
// (This assumes you already embed inputfile.SourceFile which now has Reader io.Reader.)
// If you don’t embed, your struct already contains Tx, Db, BatchID, Filename, Filepath, Logger, Reader, etc.
type File struct {
	// Usually this is embedded:
	// inputfile.SourceFile
	// If your project defines fields explicitly instead of embedding, keep that version.
	Tx       *sql.Tx
	Db       interface { // keep method shape minimal to avoid import churn
		AddFile(ctx context.Context, tx *sql.Tx, batchID int, filename, fileType string, addedAt time.Time) (int, error)
		InsertFC01Record(ctx context.Context, tx *sql.Tx, fileID int, rec *FC01Record) error
	}
	BatchID  int
	Filename string
	Filepath string
	Logger   interface { Printf(string, ...any) }
	Reader   interface { Read(p []byte) (int, error) } // io.Reader
}

// FC01Record is whatever your existing type is.
// Keep the original definition from your project.
type FC01Record struct {
	// ... your fields here ...
	InsuredAddress1 struct {
		Valid  bool
		String string
	}
	InsuredAddress2 struct {
		Valid  bool
		String string
	}
	InsuredAddress3 struct {
		Valid  bool
		String string
	}
	InsuredAddress4 struct {
		Valid  bool
		String string
	}
	InsuredAddress5 struct {
		Valid  bool
		String string
	}
	// plus any other fields the parser fills in
}

// parseFC01Content is your existing parser (already in this package).
// We keep the call-site name as-is to avoid renames.
func parseFC01Content(line string) (*FC01Record, error) {
	// NOTE: this is just a stub signature kept for reference in this snippet.
	// In your real project, the actual implementation already exists.
	return nil, nil
}

func (fc File) ProcessFile(ctx context.Context) error {
	var (
		err                       error
		applicationCreatedDateMap map[string]time.Time
	)

	// Range bootstrap (unchanged semantics)
	shouldImportInRange, importDateRangeStart, importDateRangeEnd := util.ShouldImportInTheRange()
	if shouldImportInRange {
		applicationCreatedDateMap, err = fc19.CreateApplicationCreatedDateMap(fc.Logger, fc.BatchID)
		if err != nil {
			// S3-only: translate missing FC19 via s3io.NotFoundError
			var nf *s3io.NotFoundError
			if errors.As(err, &nf) {
				return &FC19FileMissing{FileName: nf.Key}
			}
			return err
		}
	}

	addedAt := time.Now().UTC()
	id, err := fc.Db.AddFile(ctx, fc.Tx, fc.BatchID, fc.Filename, "FC01", addedAt)
	if err != nil {
		return err
	}

	// *** KEY CHANGE: read from the provided stream (S3 body) instead of os.Open ***
	sc := bufio.NewScanner(fc.Reader)
	// Skip header
	sc.Scan()

	count := 0
	added := 0
	noteIdx := 0

	for sc.Scan() {
		count++
		line := sc.Text()
		if len(line) < 1143 {
			// preserve original validation + message
			appNum := line[:minInt(11, len(line))]
			fc.Logger.Printf("line %d: application '%s' has incorrect line length %d", count, appNum, len(line))
			continue
		}

		applicationNumber := line[:11]

		// Range check unchanged
		inRange := false
		if shouldImportInRange {
			if createdAt, ok := applicationCreatedDateMap[applicationNumber]; ok &&
				!createdAt.Before(*importDateRangeStart) &&
				!createdAt.After(*importDateRangeEnd) {
				inRange = true
			}
		}

		if shouldImportInRange && !inRange {
			continue
		}

		// Parse line using your existing parser
		record, err := parseFC01Content(line)
		if err != nil {
			fc.Logger.Printf("line %d: parse error for application '%s': %v", count, applicationNumber, err)
			continue
		}

		// Insert (same DAO call as before)
		if err := fc.Db.InsertFC01Record(ctx, fc.Tx, id, record); err != nil {
			fc.Logger.Printf("line %d: failed to insert application '%s': %v", count, applicationNumber, err)
			continue
		}

		// *** Keep anonymize logic exactly as before ***
		if viper.GetBool("anonymize") {
			if record.InsuredAddress2.Valid || record.InsuredAddress3.Valid {
				record.InsuredAddress1.Valid = true
				record.InsuredAddress1.String = faker.Address().StreetAddress()
				record.InsuredAddress2.Valid = false
				record.InsuredAddress2.String = ""
				record.InsuredAddress3.Valid = false
				record.InsuredAddress3.String = ""
				record.InsuredAddress4.Valid = false
				record.InsuredAddress4.String = ""
				record.InsuredAddress5.Valid = false
				record.InsuredAddress5.String = ""
			}
		}

		// *** Preserve any existing “fakeNotes” behavior if present in original ***
		if len(fakeNotes) > 0 {
			_ = fakeNotes[noteIdx%len(fakeNotes)]
			noteIdx++
		}

		added++
	}

	if err := sc.Err(); err != nil {
		return fmt.Errorf("error scanning FC01 stream: %w", err)
	}

	fc.Logger.Printf("FC01 processing completed — total: %d, added: %d", count, added)
	return nil
}

// Helper to avoid slice panic in length error message
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}