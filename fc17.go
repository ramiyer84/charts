package fc17

import (
	"bufio"
	"context"
	"database/sql"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/logger"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/util"
	"github.com/spf13/viper"
	"path/filepath"
	"strings"
	"time"
)

type File struct {
	*inputfile.SourceFile
}

// Source table name: KJDTLED
type FC17Record struct {
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
	addedAt := time.Now().UTC()
	id, err := fc.Db.AddFile(ctx, fc.Tx, fc.BatchID, fc.Filename, "FC19", addedAt)
	if err != nil {
		return err
	}

	r := bufio.NewScanner(fc.Reader)
	// Skip the first line
	r.Scan()

	count := 0
	added := 0
	for r.Scan() {
		count++
		line := r.Text()
		applicationNumber := line[:11]
		if len(line) < 119 {
			fc.Logger.Printf("line %d: application '%s' has incorrect line length %d", count, applicationNumber, len(line))
			continue
		}

		if fc.Range.ApplicationNumberExists(applicationNumber) {
			record, err := parseFC19Content(line)
			if err != nil {
				fc.Logger.Printf("cannot parse F19 line %d (%s): %v", count, line, err)
				return err
			}

			err = addFC17Record(ctx, fc.Tx, id, record, addedAt)
			if err != nil {
				fc.Logger.Printf("cannot add F19 record to Database from line %d (%s): %v", count, line, err)
				return err
			}
			added++
		}
	}

	err = fc.Db.UpdateFileStatus(ctx, fc.Tx, "IMPORTED", id, time.Now().UTC())
	if err != nil {
		return err
	}

	fc.Logger.Printf("completed processing '%s' file, loaded %d records", fc.Filename, added)
	return nil
}

func parseFC19Content(line string) (*FC17Record, error) {
	data := []rune(line)

	var err error
	// Application Number
	tmp := strings.TrimSpace(util.SubstringBeginning(data, 11))
	record := FC17Record{
		ApplicationNumber: tmp,
	}
	// Application Status
	record.ApplicationStatus = util.ReadNullString(util.Substring(data, 12, 15))
	// Status Creation Date
	record.StatusCreationDate, err = util.ReadMacaoNullTimestamp(util.Substring(data, 16, 42))
	if err != nil {
		return nil, err
	}
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

func CreateApplicationCreatedDateMap(l *logger.Logger, batchId string) (map[string]time.Time, error) {
	filePath := filepath.Join(viper.GetString("location.input"), "SLMD_MACAOFC19_"+batchId+".txt")
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	result := make(map[string]time.Time)
	r := bufio.NewScanner(f)
	// Skip the first line
	r.Scan()

	count := 0
	for r.Scan() {
		count++
		line := r.Text()
		if len(line) < 119 {
			applicationNumber := line[:11]
			l.Printf("line %d: application '%s' has incorrect line length %d", count, applicationNumber, len(line))
			continue
		}

		record, err := parseFC19Content(line)
		if err != nil {
			l.Printf("cannot parse F19 line %d (%s): %v", count, line, err)
			return nil, err
		}

		if err != nil {
			l.Printf("cannot add F19 record to Database from line %d (%s): %v", count, line, err)
			return nil, err
		}

		if record.ApplicationStatus.Valid && record.ApplicationStatus.String == "CRE" && record.StatusCreationDate.Valid {
			result[record.ApplicationNumber] = record.StatusCreationDate.Time
		}
	}

	return result, nil
}

func addFC17Record(ctx context.Context, tx *sql.Tx, fileId uint, record *FC17Record, addedAt time.Time) error {
	var id uint
	err := tx.QueryRowContext(ctx, "INSERT INTO FC19_RECORDS (ID, FILE_ID, APPLICATION_NUMBER, APPLICATION_STATUS, "+
		"STATUS_CREATION_DATE, REMOTE_TRANSMISSION, RECIPIENT_CODE_TYPE, PROTESTOR_CODE_TYPE, DECISION_TYPE_CODE, "+
		"CREATED_AT) VALUES (NEXTVAL('FC19_SEQ'), $1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING ID",
		fileId, record.ApplicationNumber, record.ApplicationStatus, record.StatusCreationDate, record.RemoteTransmission,
		record.RecipientCodeType, record.ProtestorCodeType, record.DecisionTypeCode, addedAt).Scan(&id)

	if err != nil {
		return err
	}

	return nil
}
