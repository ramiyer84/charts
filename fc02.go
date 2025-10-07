package fc02

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

// Source table name: KJDTEMP
type FC02Record struct {
	ApplicationNumber         string          // line[0]  (IDGMDO)
	LoanID                    int64           // line[1]  (IDGMEM)
	LoanNumber                sql.NullString  // line[2]  (NOGMPR)
	LoanNumberKey             sql.NullString  // line[3]  (CKPAPT)
	RPP                       sql.NullString  // line[4]  (IDCORP)
	EmployeeTypeCode          sql.NullString  // line[5]  (CCCOCP)
	AdhesionDate              sql.NullTime    // line[6]  (DDGMAH)
	LoanCapitalAmount         sql.NullInt64   // line[7]  (MKGMPR)
	LoanDurationMonths        sql.NullString  // line[8]  (PMGMPR)
	InsuredCapitaRate         sql.NullFloat64 // line[9]  (TXGMPA)
	LoanType                  sql.NullString  // line[10] (CYGMPR)
	ProfessionCode            sql.NullString  // line[11] (CCGMST)
	CFF                       sql.NullString  // line[12] (CEGMEC)
	BorrowerStatus            sql.NullString  // line[13] (CYGMEM)
	PremiumCalledRate         sql.NullFloat64 // line[14] (TXGMAP)
	NormalRate                sql.NullFloat64 // line[15] (TXGMPN)
	OverPremiumRate           sql.NullFloat64 // line[16] (TXGMSU)
	LoanReferenceNumber       sql.NullString  // line[17] (LCGMPR)
	CurrencyCode              sql.NullString  // line[18] (CDSIMI)
	CFFLoanNumber             sql.NullString  // line[19] (NOGMPI)
	ProductCode               sql.NullString  // line[20] (CDPREM)
	NormalDeathRate           sql.NullFloat64 // line[21] (TXGMPS)
	NormalInvalidityRate      sql.NullFloat64 // line[22] (TXGMPI)
	OverpremiumDeathRate      sql.NullFloat64 // line[23] (TXGMSS)
	OverpremiumInvalidityRate sql.NullFloat64 // line[24] (TXGMSI)
	GISAerasCover             sql.NullString  // line[25] (CDGMIV)
	AerasCapping              sql.NullString  // line[26] (CDGMEC)
	OngoingAmount             sql.NullInt64   // line[27] (MTENCO)
}

func (fc File) AsyncProcessFile(ctx context.Context) {
	fc.Process(ctx, fc.ProcessFile)
}

func (fc File) ProcessFile(ctx context.Context) error {
if err != nil {
		return err
	}
	addedAt := time.Now().UTC()
	id, err := fc.Db.AddFile(ctx, fc.Tx, fc.BatchID, fc.Filename, "FC02", addedAt)
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
		if len(line) < 470 {
			fc.Logger.Printf("line %d: application '%s' has incorrect line length %d", count, applicationNumber, len(line))
			continue
		}

		if fc.Range.ApplicationNumberExists(applicationNumber) {
			record, err := parseFC02Content(line)
			if err != nil {
				applicationNumber := line[:11]
				fc.Logger.Printf("line %d: line length %d, cannot parse F01 line for application %s (%s): %v", count, len(line), applicationNumber, line, err)
				err = nil
				continue
			}

			err = addFC02Record(ctx, fc.Tx, id, record, addedAt)
			if err != nil {
				fc.Logger.Printf("cannot add F02 record to Database from line %d (%s): %v", count, line, err)
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

func parseFC02Content(line string) (*FC02Record, error) {
	data := []rune(line)
	// Application Number
	tmp := strings.TrimSpace(util.SubstringBeginning(data, 11))
	record := FC02Record{
		ApplicationNumber: tmp,
	}
	// Loan ID
	n, err := util.ReadInt64(util.Substring(data, 12, 21))
	if err != nil {
		return nil, err
	}
	record.LoanID = n
	// Loan Number
	record.LoanNumber = util.ReadNullString(util.Substring(data, 22, 38))
	// Loan Number Key
	record.LoanNumberKey = util.ReadNullString(util.Substring(data, 39, 40))
	// RPP
	record.RPP = util.ReadNullString(util.Substring(data, 41, 54))
	// Employee Type Code
	record.EmployeeTypeCode = util.ReadNullString(util.Substring(data, 55, 57))
	// Adhesion Date
	record.AdhesionDate, err = util.ReadDB2NullDate(util.Substring(data, 58, 68))
	if err != nil {
		return nil, err
	}
	// Loan Capital Amount
	record.LoanCapitalAmount, err = util.ReadNullInt64(util.Substring(data, 69, 78))
	if err != nil {
		return nil, err
	}
	// Loan Duration Months
	record.LoanDurationMonths = util.ReadNullString(util.Substring(data, 79, 82))
	// Insured Capita Rate
	record.InsuredCapitaRate, err = util.ReadNullFloat64(util.Substring(data, 83, 89))
	if err != nil {
		return nil, err
	}
	// Loan Duration Months
	record.LoanType = util.ReadNullString(util.Substring(data, 90, 92))
	// Profession Code
	record.ProfessionCode = util.ReadNullString(util.Substring(data, 93, 96))
	// CFF
	record.CFF = util.ReadNullString(util.Substring(data, 97, 99))
	// Borrower Status
	record.BorrowerStatus = util.ReadNullString(util.Substring(data, 100, 101))
	// Premium Called Rate
	record.PremiumCalledRate, err = util.ReadNullFloat64(util.Substring(data, 102, 108))
	if err != nil {
		return nil, err
	}
	// Normal Rate
	record.NormalRate, err = util.ReadNullFloat64(util.Substring(data, 109, 115))
	if err != nil {
		return nil, err
	}
	// Over Premium Rate
	record.OverPremiumRate, err = util.ReadNullFloat64(util.Substring(data, 116, 122))
	if err != nil {
		return nil, err
	}
	// Loan Reference Number
	record.LoanReferenceNumber = util.ReadNullString(util.Substring(data, 123, 139))
	// Currency Code
	record.CurrencyCode = util.ReadNullString(util.Substring(data, 140, 143))
	// CFF Loan Number
	record.CFFLoanNumber = util.ReadNullString(util.Substring(data, 144, 160))
	// Product Code
	record.ProductCode = util.ReadNullString(util.Substring(data, 161, 169))
	// Normal Death Rate
	record.NormalDeathRate, err = util.ReadNullFloat64(util.Substring(data, 170, 176))
	if err != nil {
		return nil, err
	}
	// Normal Invalidity Rate
	record.NormalInvalidityRate, err = util.ReadNullFloat64(util.Substring(data, 177, 183))
	if err != nil {
		return nil, err
	}
	// Over Premium Death Rate
	record.OverpremiumDeathRate, err = util.ReadNullFloat64(util.Substring(data, 184, 190))
	if err != nil {
		return nil, err
	}
	// Over Premium Invalidity Rate
	record.OverpremiumInvalidityRate, err = util.ReadNullFloat64(util.Substring(data, 191, 197))
	if err != nil {
		return nil, err
	}
	// GIS Aeras Cover
	record.GISAerasCover = util.ReadNullString(util.Substring(data, 198, 199))
	// Aeras Capping
	record.AerasCapping = util.ReadNullString(util.Substring(data, 200, 201))
	// Ongoing Amount
	record.OngoingAmount, err = util.ReadNullInt64(util.SubstringEnd(data, 202))
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func addFC02Record(ctx context.Context, tx *sql.Tx, fileId uint, record *FC02Record, addedAt time.Time) error {
	var id uint
	err := tx.QueryRowContext(ctx, "INSERT INTO FC02_RECORDS (ID, FILE_ID, APPLICATION_NUMBER, LOAN_ID, LOAN_NUMBER, "+
		"LOAN_NUMBER_KEY, RPP, EMPLOYEE_TYPE_CODE, ADHESION_DATE, "+
		"LOAN_CAPITAL_AMOUNT, LOAN_DURATION_MONTHS, INSURED_CAPITA_RATE, LOAN_TYPE, PROFESSION_CODE, CFF, "+
		"BORROWER_STATUS, PREMIUM_CALLED_RATE, NORMAL_RATE, OVERPREMIUM_RATE, LOAN_REF_NUMBER, CURRENCY_CODE, "+
		"CFF_LOAN_NUMBER, PRODUCT_CODE, NORMAL_DEATH_RATE, NORMAL_INVALIDITY_RATE, OVERPREMIUM_DEATH_RATE, "+
		"OVERPREMIUM_INVALIDITY_RATE, GIS_AERAS_COVER, AERAS_CAPPING, ONGOING_AMOUNT, CREATED_AT) VALUES (NEXTVAL('FC02_SEQ'), "+
		"$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, "+
		"$25, $26, $27, $28, $29, $30) RETURNING ID",
		fileId, record.ApplicationNumber, record.LoanID, record.LoanNumber, record.LoanNumberKey, record.RPP, record.EmployeeTypeCode,
		record.AdhesionDate, record.LoanCapitalAmount, record.LoanDurationMonths, record.InsuredCapitaRate, record.LoanType,
		record.ProfessionCode, record.CFF, record.BorrowerStatus, record.PremiumCalledRate, record.NormalRate, record.OverPremiumRate,
		record.LoanReferenceNumber, record.CurrencyCode, record.CFFLoanNumber, record.ProductCode, record.NormalDeathRate,
		record.NormalInvalidityRate, record.OverpremiumDeathRate, record.OverpremiumInvalidityRate, record.GISAerasCover,
		record.AerasCapping, record.OngoingAmount, addedAt).Scan(&id)

	if err != nil {
		return err
	}

	return nil
}