package inputfile

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
)

// File represents the FC01 file structure (unchanged)
type File struct {
	Tx       *sql.Tx
	Db       *DB
	BatchID  int
	Filename string
	Filepath string
	Logger   *Logger
	Reader   io.Reader
}

// FC19FileMissing indicates the FC19 file could not be found in S3.
type FC19FileMissing struct {
	FileName string
}

func (e *FC19FileMissing) Error() string {
	return fmt.Sprintf("FC19 file missing in S3: %s", e.FileName)
}

func (fc File) ProcessFile(ctx context.Context) error {
	var (
		err                       error
		applicationCreatedDateMap map[string]time.Time
	)

	// --- Handle optional import range filter based on FC19 data ---
	shouldImportInRange, importDateRangeStart, importDateRangeEnd := util.ShouldImportInTheRange()
	if shouldImportInRange {
		applicationCreatedDateMap, err = fc19.CreateApplicationCreatedDateMap(fc.Logger, fc.BatchID)
		if err != nil {
			// S3-only: detect missing FC19 file via s3io.NotFoundError
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

	// --- Read directly from the S3 stream provided by fc.Reader ---
	r := bufio.NewScanner(fc.Reader)
	r.Scan() // skip header

	count := 0
	added := 0
	for r.Scan() {
		count++
		line := r.Text()
		applicationNumber := line[:11]

		// Basic validation
		if len(line) < 1143 {
			fc.Logger.Printf("line %d: application '%s' has incorrect line length %d", count, applicationNumber, len(line))
			continue
		}

		// --- Range check based on FC19 created dates (if enabled) ---
		inRange := false
		if shouldImportInRange {
			createdAt, ok := applicationCreatedDateMap[applicationNumber]
			if ok && !createdAt.Before(*importDateRangeStart) && !createdAt.After(*importDateRangeEnd) {
				inRange = true
			}
		}

		if !shouldImportInRange || inRange {
			record, err := ParseFC01Line(line)
			if err != nil {
				fc.Logger.Printf("line %d: parse error for application '%s': %v", count, applicationNumber, err)
				continue
			}

			err = fc.Db.InsertFC01Record(ctx, fc.Tx, id, record)
			if err != nil {
				fc.Logger.Printf("line %d: failed to insert application '%s': %v", count, applicationNumber, err)
				continue
			}

			added++
		}
	}

	if err := r.Err(); err != nil {
		return fmt.Errorf("error scanning FC01 stream: %w", err)
	}

	fc.Logger.Printf("FC01 processing completed — total: %d, added: %d", count, added)
	return nil
}