package fc10

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

// Source table name: KJDTPO2
type FC10Record struct {
	ApplicationNumber     string         // line[0]  (IDGMDO)
	LoanID                int64          // line[1]  (IDGMEM)
	CoverNumber           sql.NullInt64  // line[2]  (IDGMGE)
	DecisionType          sql.NullString // line[3]  (CDGMDE)
	CreationDate          sql.NullTime   // line[4]  (DDCHP2)
	DecisionLevel         sql.NullString // line[5]  (CSGMND)
	BeneficiaryReasonCode sql.NullString // line[6]  (CDGMRB)
	ReasonCode            sql.NullString // line[7]  (CDGMRE)
	SecondReasonCode      sql.NullString // line[8]  (CDGMR2)
	AdjournmentDate       sql.NullString // line[9]  (LMGMAJ)
	ReserveDate1          sql.NullString // line[10] (LMGMR1)
	ReserveDate2          sql.NullString // line[11] (LMGMR2)
	MedicalDecisionCode   sql.NullString // line[12] (CDGMDM)
}

func (fc File) AsyncProcessFile(ctx context.Context) {
	fc.Process(ctx, fc.ProcessFile)
}

func (fc File) ProcessFile(ctx context.Context) error {

	addedAt := time.Now().UTC()
	id, err := fc.Db.AddFile(ctx, fc.Tx, fc.BatchID, fc.Filename, "FC10", addedAt)

	r := bufio.NewScanner(fc.Reader)
	// Skip the first line
	r.Scan()

	count := 0
	added := 0
	for r.Scan() {
		count++
		line := r.Text()
		applicationNumber := line[:11]
		if len(line) < 290 {
			fc.Logger.Printf("line %d: application '%s' has incorrect line length %d", count, applicationNumber, len(line))
			continue
		}

		if fc.Range.ApplicationNumberExists(applicationNumber) {
			record, err := parseFC10Content(line)
			if err != nil {
				fc.Logger.Printf("cannot parse F10 line %d (%s): %v", count, line, err)
				return err
			}

			err = addFC10Record(ctx, fc.Tx, id, record, addedAt)
			if err != nil {
				fc.Logger.Printf("cannot add F10 record to Database from line %d (%s): %v", count, line, err)
				return err
			}
			added++
		}
	}

	err = fc.Db.UpdateFileStatus(ctx, fc.Tx, "IMPORTED", id, time.Now().UTC())

	fc.Logger.Printf("completed processing '%s' file, loaded %d records", fc.Filename, added)
	return nil
}

func parseFC10Content(line string) (*FC10Record, error) {
	data := []rune(line)

	// Application Number
	tmp := strings.TrimSpace(util.SubstringBeginning(data, 11))
	record := FC10Record{
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
	// Decision Type
	record.DecisionType = util.ReadNullString(util.Substring(data, 32, 36))
	// Creation Date
	record.CreationDate, err = util.ReadMacaoNullTimestamp(util.Substring(data, 37, 63))
	if err != nil {
		return nil, err
	}
	// Decision Level
	record.DecisionLevel = util.ReadNullString(util.Substring(data, 64, 65))
	// Beneficiary Reason Code
	record.BeneficiaryReasonCode = util.ReadNullString(util.Substring(data, 66, 70))
	// Reason Code
	record.ReasonCode = util.ReadNullString(util.Substring(data, 71, 75))
	// Second Reason Code
	record.SecondReasonCode = util.ReadNullString(util.Substring(data, 76, 80))
	// Adjournment Date
	record.AdjournmentDate = util.ReadNullString(util.Substring(data, 81, 89))
	// Reserve Date 1
	record.ReserveDate1 = util.ReadNullString(util.Substring(data, 90, 98))
	// Reserve Date 2
	record.ReserveDate2 = util.ReadNullString(util.Substring(data, 99, 107))
	// Medical Decision Code
	record.MedicalDecisionCode = util.ReadNullString(util.SubstringEnd(data, 108))

	return &record, nil
}

func addFC10Record(ctx context.Context, tx *sql.Tx, fileId uint, record *FC10Record, addedAt time.Time) error {
	var id uint
	err := tx.QueryRowContext(ctx, "INSERT INTO FC10_RECORDS (ID, FILE_ID, APPLICATION_NUMBER, LOAN_ID, COVER_NUMBER, "+
		"DECISION_TYPE, CREATION_DATE, DECISION_LEVEL, BENEFICIARY_REASON_CODE, REASON_CODE, SECOND_REASON_CODE, "+
		"ADJOURNMENT_DATE, RESERVE_DATE_1, RESERVE_DATE_2, MEDICAL_DECISION_CODE, CREATED_AT) VALUES (NEXTVAL('FC10_SEQ'), "+
		"$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) RETURNING ID",
		fileId, record.ApplicationNumber, record.LoanID, record.CoverNumber, record.DecisionType, record.CreationDate, record.DecisionLevel,
		record.BeneficiaryReasonCode, record.ReasonCode, record.SecondReasonCode, record.AdjournmentDate, record.ReserveDate1,
		record.ReserveDate2, record.MedicalDecisionCode, addedAt).Scan(&id)

	return nil
}