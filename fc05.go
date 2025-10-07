package fc05

import (
	"bufio"
	"context"
	"database/sql"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/util"
	"strings"
	"time"
)

type File struct {
	*inputfile.SourceFile
}

// Source table name: KJDTGEM
type FC05Record struct {
	ApplicationNumber string         // line[0]  (IDGMDO)
	LoanID            int64          // line[1]  (IDGMEM)
	CoverNumber       sql.NullInt64  // line[2]  (IDGMGE)
	CoverType         sql.NullString // line[3]  (CCGMGA)
	CoverLabel        sql.NullString // line[4]  (LLGMGA)
	OptionalCoverCode sql.NullString // line[5]  (CEGMGO)
	InsuredAmount     sql.NullInt64  // line[6]  (MTGMAS)
	DoubleEffectCode  sql.NullString // line[7]  (CEGMDE)
	WaitingPeriodDays sql.NullString // line[8]  (PJGMFR)
	Submitted         sql.NullString // line[9]  (CEGMGS)
	CurrencyCode      sql.NullString // line[10] (CDSIMI)
}

func (fc File) AsyncProcessFile(ctx context.Context) {
	fc.Process(ctx, fc.ProcessFile)
}

func (fc File) ProcessFile(ctx context.Context) error {
	addedAt := time.Now().UTC()
	id, err := fc.Db.AddFile(ctx, fc.Tx, fc.BatchID, fc.Filename, "FC05", addedAt)
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
		if len(line) < 250 {
			fc.Logger.Printf("line %d: application '%s' has incorrect line length %d", count, applicationNumber, len(line))
			continue
		}

		if fc.Range.ApplicationNumberExists(applicationNumber) {
			record, err := parseFC05Content(line)
			if err != nil {
				fc.Logger.Printf("cannot parse F05 line %d (%s): %v", count, line, err)
				return err
			}

			err = addFC05Record(ctx, fc.Tx, id, record, addedAt)
			if err != nil {
				fc.Logger.Printf("cannot add F05 record to Database from line %d (%s): %v", count, line, err)
				return err
			}
			added++
		}
	}

	err = fc.Db.UpdateFileStatus(ctx, fc.Tx, "IMPORTED", id, time.Now().UTC())
	if err != nil {
		return err
	}

	fc.Logger.Printf("completed processing '%s' file: loaded %d records", fc.Filename, added)
	return nil
}

func parseFC05Content(line string) (*FC05Record, error) {
	data := []rune(line)
	// Application Number
	tmp := strings.TrimSpace(util.SubstringBeginning(data, 11))
	record := FC05Record{
		ApplicationNumber: tmp,
	}
	// Loan ID
	n, err := util.ReadInt64(util.Substring(data, 12, 21))
	if err != nil {
		return nil, err
	}
	record.LoanID = n
	// Cover Number
	record.CoverNumber, err = util.ReadNullInt64(util.Substring(data, 22, 31))
	if err != nil {
		return nil, err
	}
	// Cover Type
	record.CoverType = util.ReadNullString(util.Substring(data, 32, 35))
	// Cover Label
	record.CoverLabel = util.ReadNullString(util.Substring(data, 36, 86))
	// Optional Cover Code
	record.OptionalCoverCode = util.ReadNullString(util.Substring(data, 87, 88))
	// Insured Amount
	record.InsuredAmount, err = util.ReadNullInt64(util.Substring(data, 89, 98))
	if err != nil {
		return nil, err
	}
	// Double Effect Code
	record.DoubleEffectCode = util.ReadNullString(util.Substring(data, 99, 100))
	// Waiting Period Days
	record.WaitingPeriodDays = util.ReadNullString(util.Substring(data, 101, 104))
	// Submitted
	record.Submitted = util.ReadNullString(util.Substring(data, 105, 106))
	// Currency Code
	record.CurrencyCode = util.ReadNullString(util.SubstringEnd(data, 107))

	return &record, nil
}

func addFC05Record(ctx context.Context, tx *sql.Tx, fileId uint, record *FC05Record, addedAt time.Time) error {
	var id uint
	err := tx.QueryRowContext(ctx, "INSERT INTO FC05_RECORDS (ID, FILE_ID, APPLICATION_NUMBER, LOAN_ID, COVER_NUMBER, "+
		"COVER_TYPE, COVER_LABEL, OPTIONAL_COVER_CODE, INSURED_AMOUNT, DOUBLE_EFFECT_COVER, WAITING_PERIOD_DAYS, SUBMITTED, "+
		"CURRENCY_CODE, CREATED_AT) VALUES (NEXTVAL('FC05_SEQ'), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, "+
		"$11, $12, $13) RETURNING ID",
		fileId, record.ApplicationNumber, record.LoanID, record.CoverNumber, record.CoverType, record.CoverLabel,
		record.OptionalCoverCode, record.InsuredAmount, record.DoubleEffectCode, record.WaitingPeriodDays, record.Submitted,
		record.CurrencyCode, addedAt).Scan(&id)

	if err != nil {
		return err
	}

	return nil
}