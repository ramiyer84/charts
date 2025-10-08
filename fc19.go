package fc19

import (
	"bufio"
	"context"
	"database/sql"
	"strings"
	"time"

	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/util"
)

// File is the FC19 processor. It consumes the provided Reader and writes to DB.
type File struct {
	*inputfile.SourceFile
	Range *inputfile.Range // optional; worker may set a cache to validate once
}

// Source table name: KJDTLED
type FC19Record struct {
	ApplicationNumber  string         // line[0] (IDGMDO)
	ApplicationStatus  sql.NullString // line[1] (CEGMDO)
	StatusCreationDate sql.NullTime   // line[2] (DDCHET)
	RemoteTransmission sql.NullString // line[3] (CEGMET)
	RecipientCodeType  sql.NullString // line[4] (CYGMDE)
	ProtestorCodeType  sql.NullString // line[5] (CYGMCO)
	DecisionTypeCode   sql.NullString // line[6] (CDGMDE)
}

func (fc File) AsyncProcessFile(ctx context.Context) {
	fc.Process(ctx, fc.ProcessFile)
}

func (fc File) ProcessFile(ctx context.Context) error {
	// If worker didn't validate ranges, do it here once.
	// NOTE: Range.Validate in the original codebase does not return an error.
	if fc.Range != nil && !fc.Range.Loaded {
		fc.Range.Validate(ctx, fc.Db, fc.Tx, fc.BatchID)
	}

	addedAt := time.Now().UTC()
	id, err := fc.Db.AddFile(ctx, fc.Tx, fc.BatchID, fc.Filename, "FC19", addedAt)
	if err != nil {
		return err
	}

	r := bufio.NewScanner(fc.Reader)
	// Skip the first line (header)
	r.Scan()

	count := 0
	added := 0
	for r.Scan() {
		count++
		line := r.Text()

		// Basic length check & application number slice protection
		if len(line) < 119 {
			applicationNumber := ""
			if len(line) >= 11 {
				applicationNumber = line[:11]
			}
			fc.Logger.Printf("line %d: application '%s' has incorrect line length %d",
				count, applicationNumber, len(line))
			continue
		}

		applicationNumber := line[:11]
		if fc.Range.ApplicationNumberExists(applicationNumber) {
			record, err := parseFC19Content(line)
			if err != nil {
				fc.Logger.Printf("cannot parse FC19 line %d (%s): %v", count, line, err)
				return err
			}

			if err := addFC19Record(ctx, fc.Tx, id, record, addedAt); err != nil {
				fc.Logger.Printf("cannot add FC19 record from line %d (%s): %v", count, line, err)
				return err
			}
			added++
		}
	}

	if err := r.Err(); err != nil {
		return err
	}

	if err := fc.Db.UpdateFileStatus(ctx, fc.Tx, "IMPORTED", id, time.Now().UTC()); err != nil {
		return err
	}

	fc.Logger.Printf("completed processing '%s' file, loaded %d records", fc.Filename, added)
	return nil
}

func parseFC19Content(line string) (*FC19Record, error) {
	data := []rune(line)

	// Application Number
	tmp := strings.TrimSpace(util.SubstringBeginning(data, 11))
	record := FC19Record{
		ApplicationNumber: tmp,
	}

	// Application Status
	record.ApplicationStatus = util.ReadNullString(util.Substring(data, 12, 15))

	// Status Creation Date
	ts, err := util.ReadMacaoNullTimestamp(util.Substring(data, 16, 42))
	if err != nil {
		return nil, err
	}
	record.StatusCreationDate = ts

	// Remote Transmission
	record.RemoteTransmission = util.ReadNullString(util.Substring(data, 43, 44))

	// Recipient Code Type
	record.RecipientCodeType = util.ReadNullString(util.Substring(data, 45, 46))

	// Protestor Code Type
	record.ProtestorCodeType = util.ReadNullString(util.Substring(data, 47, 48))

	// Decision Type Code
	record.DecisionTypeCode = util.ReadNullString(util.SubstringEnd(data, 49))

	return &record, nil
}

func addFC19Record(ctx context.Context, tx *sql.Tx, fileId uint, record *FC19Record, addedAt time.Time) error {
	var id uint
	err := tx.QueryRowContext(ctx,
		"INSERT INTO FC19_RECORDS (ID, FILE_ID, APPLICATION_NUMBER, APPLICATION_STATUS, "+
			"STATUS_CREATION_DATE, REMOTE_TRANSMISSION, RECIPIENT_CODE_TYPE, PROTESTOR_CODE_TYPE, DECISION_TYPE_CODE, "+
			"CREATED_AT) VALUES (NEXTVAL('FC19_SEQ'), $1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING ID",
		fileId,
		record.ApplicationNumber,
		record.ApplicationStatus,
		record.StatusCreationDate,
		record.RemoteTransmission,
		record.RecipientCodeType,
		record.ProtestorCodeType,
		record.DecisionTypeCode,
		addedAt,
	).Scan(&id)

	if err != nil {
		return err
	}
	return nil
}
