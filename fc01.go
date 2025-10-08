package fc01

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.axa.com/axa-partners-clp/mrt-shared/encryption"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/util"
	"github.com/spf13/viper"
	"strconv"
	"strings"
	"syreclabs.com/go/faker"
	"time"
)

var (
	fakeNotes = [...]string{
		"REFUS 2E NIV 20/11/2020 EV. Applicaton: '%v'",
		"DOSSIER ANNULE ET REMPLACE PAR LE DOSSIER 2020A047346 * MB 19/10/20. Applicaton: '%v'",
		"RENVOI DRC PAR MAIL CRYPTE AU CLIENT * MP LE 29/06/2020*. Applicaton: '%v'",
		"REFUS DANS LE CADRE DU 2EME NIVEAU D'ASSURANCE. Applicaton: '%v'",
	}
)

type File struct {
	*inputfile.SourceFile
}

type FC19FileMissing struct {
	FileName string
}

func (m *FC19FileMissing) Error() string {
	return "corresponding F19 file '" + m.FileName + "' is missing"
}

// Source table name: KJDTDOS
type FC01Record struct {
	ApplicationNumber                string         // line[0]  (IDGMDO)
	TypeOfMethod                     int64          // line[1]  (IDGMME)
	InsuredID                        int64          // line[2]  (IDGMAS)
	AgentBrokerNumber                string         // line[3]  (CCINAX)
	InspectorCode                    string         // line[4]  (CDINIS)
	NotUsedCreditor                  sql.NullString // line[5]  (CDINEX)
	UserUpdateName                   sql.NullString // line[6]  (ZCOENT)
	UserCode                         sql.NullString // line[7]  (ZCOPID)
	PMID                             sql.NullInt64  // line[8]  (IDPMZZ)
	AgentName                        sql.NullString // line[9]  (LNGMCT)
	BankName                         sql.NullString // line[10] (LNGMAB)
	AgentAddress2                    sql.NullString // line[11] (LRGMC2)
	AgentAddress3                    sql.NullString // line[12] (LRGMC3)
	AgentAddress4                    sql.NullString // line[13] (LRGMC4)
	AgentAddress5                    sql.NullString // line[14] (LRGMC5)
	AgentPostCode                    sql.NullString // line[15] (CPGMC6)
	AgentTown                        sql.NullString // line[16] (LVGMC6)
	AgentCountryCode                 sql.NullString // line[17] (CDPACT)
	AgentID                          sql.NullInt64  // line[18] (IDGMAB)
	BusinessDepartmentCode           sql.NullString // line[19] (IDGMAB)
	BusinessDepartmentCodeForInvoice sql.NullString // line[20] (ZCOSFA)
	ApplicationLastUpdatedDate       sql.NullTime   // line[21] (DMGMDO)
	ApplicationNotes                 sql.NullString // line[22] (LLGMDO) - encrypt, anonymize
	ContractReference                sql.NullString // line[23] (LCGMCO)
	ApplicantReference               sql.NullString // line[24] (LCGMPO)
	InsuredAddress1                  sql.NullString // line[25] (LNGMA1) - encrypt, anonymize
	InsuredAddress2                  sql.NullString // line[26] (LRGMA2) - encrypt, anonymize
	InsuredAddress3                  sql.NullString // line[27] (LRGMA3) - encrypt, anonymize
	InsuredAddress4                  sql.NullString // line[28] (LRGMA4) - encrypt, anonymize
	InsuredAddress5                  sql.NullString // line[29] (LRGMA5) - encrypt, anonymize
	InsuredPostCode                  sql.NullString // line[30] (CPGMA6)
	InsuredTown                      sql.NullString // line[31] (LVGMA6) - encrypt, anonymize
	InsuredCountry                   sql.NullString // line[32] (CDPAPP)
	ThirdPartyFullName               sql.NullString // line[33] (CDPAPP) - encrypt, anonymize
	ThirdPartyAddress2               sql.NullString // line[34] (LRGMT2) - encrypt, anonymize
	ThirdPartyAddress3               sql.NullString // line[35] (LRGMT3) - encrypt, anonymize
	ThirdPartyAddress4               sql.NullString // line[36] (LRGMT4) - encrypt, anonymize
	ThirdPartyAddress5               sql.NullString // line[37] (LRGMT5) - encrypt, anonymize
	ThirdPartyPostCode               sql.NullString // line[38] (CPGMT6)
	ThirdPartyTown                   sql.NullString // line[39] (LVGMT6) - encrypt, anonymize
	ThirdPartyCountryCode            sql.NullString // line[40] (CDPATI)
	DecisionApplicationLevel         sql.NullString // line[41] (CDGMDE)
	DecisionCreatedAt                sql.NullTime   // line[42] (DDCHTD)
	DecisionUpdatedAt                sql.NullTime   // line[43] (DDCHAC)
	CFFEditorNumber                  sql.NullString // line[44] (COGMV2)
	LenderEmail                      sql.NullString // line[45] (MAILOP)
	CBPCountryCode                   sql.NullString // line[46] (CDPCB2)
}

func (fc File) AsyncProcessFile(ctx context.Context) {
	fc.Process(ctx, fc.ProcessFile)
}

func (fc File) ProcessFile(ctx context.Context) error {
	var (
		err                       error
		applicationCreatedDateMap map[string]time.Time
	)

	// Range filtering is controlled by config, but the companion FC19 map
	// must be provided by the worker/provider. If it wasn't provided,
	// we skip range filtering (backward compatible).
	shouldImportInRange, importDateRangeStart, importDateRangeEnd := util.ShouldImportInTheRange()
	if shouldImportInRange && fc.CompanionFC19 != nil {
		applicationCreatedDateMap = fc.CompanionFC19
	} else {
		shouldImportInRange = false
	}

	addedAt := time.Now().UTC()
	id, err := fc.Db.AddFile(ctx, fc.Tx, fc.BatchID, fc.Filename, "FC01", addedAt)
	if err != nil {
		return err
	}

	r := bufio.NewScanner(fc.Reader)
	// Skip the first line
	r.Scan()

	notesCount := 0
	count := 0
	added := 0
	for r.Scan() {
		count++
		line := r.Text()
		applicationNumber := line[:11]
		if len(line) < 1143 {
			fc.Logger.Printf("line %d: application '%s' has incorrect line length %d", count, applicationNumber, len(line))
			continue
		}

		inRange := false
		if shouldImportInRange {
			if createdAt, ok := applicationCreatedDateMap[applicationNumber]; ok &&
				!createdAt.Before(*importDateRangeStart) &&
				!createdAt.After(*importDateRangeEnd) {
				inRange = true
			}
		}

		if !shouldImportInRange || inRange {
			record, err := parseFC01Content(line)
			if err != nil {
				applicationNumber := line[:11]
				fc.Logger.Printf("line %d: line length %d, cannot parse F01 line for application %s (%s): %v", count, len(line), applicationNumber, line, err)
				err = nil
				continue
			}

			if viper.GetBool("anonymize") {
				if record.InsuredAddress1.Valid || record.InsuredAddress2.Valid || record.InsuredAddress3.Valid || record.InsuredAddress4.Valid || record.InsuredAddress5.Valid {
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
				if record.InsuredTown.Valid {
					record.InsuredTown.String = faker.Address().City()
				}
				if record.ThirdPartyFullName.Valid {
					record.ThirdPartyFullName.String = faker.Name().Name()
				}
				if record.ThirdPartyAddress2.Valid || record.ThirdPartyAddress3.Valid || record.ThirdPartyAddress4.Valid || record.ThirdPartyAddress5.Valid {
					record.ThirdPartyAddress2.Valid = true
					record.ThirdPartyAddress2.String = faker.Address().StreetAddress()
					record.ThirdPartyAddress3.Valid = false
					record.ThirdPartyAddress3.String = ""
					record.ThirdPartyAddress4.Valid = false
					record.ThirdPartyAddress4.String = ""
					record.ThirdPartyAddress5.Valid = false
					record.ThirdPartyAddress5.String = ""
				}
				if record.ThirdPartyTown.Valid {
					record.ThirdPartyTown.String = faker.Address().City()
				}
				if record.ApplicationNotes.Valid {
					record.ApplicationNotes.String = fmt.Sprintf(fakeNotes[notesCount], record.ApplicationNumber)
					if notesCount == len(fakeNotes)-1 {
						notesCount = 0
					} else {
						notesCount++
					}
				}
			}

			if err = addFC01Record(ctx, fc.Encryption, fc.Tx, id, record, addedAt); err != nil {
				fc.Logger.Printf("line %d: cannot add F01 record to Database line length %d (%s): %v", count, len(line), line, err)
				if len(record.AgentBrokerNumber) > 5 {
					fc.Logger.Printf("agent broker number %s", record.AgentBrokerNumber)
				}
				if len(record.InspectorCode) > 5 {
					fc.Logger.Printf("inspector code %s", record.InspectorCode)
				}
				if record.NotUsedCreditor.Valid && len(record.NotUsedCreditor.String) > 5 {
					fc.Logger.Printf("not used creditor %s", record.NotUsedCreditor.String)
				}
				if record.AgentPostCode.Valid && len(record.AgentPostCode.String) > 5 {
					fc.Logger.Printf("agent post code %s", record.AgentPostCode.String)
				}
				if record.InsuredPostCode.Valid && len(record.InsuredPostCode.String) > 5 {
					fc.Logger.Printf("insured post code %s", record.InsuredPostCode.String)
				}
				if record.ThirdPartyPostCode.Valid && len(record.ThirdPartyPostCode.String) > 5 {
					fc.Logger.Printf("third party post code %s", record.ThirdPartyPostCode.String)
				}
				if record.CFFEditorNumber.Valid && len(record.CFFEditorNumber.String) > 5 {
					fc.Logger.Printf("CFF editor number %s", record.CFFEditorNumber.String)
				}
				if record.CBPCountryCode.Valid && len(record.CBPCountryCode.String) > 5 {
					fc.Logger.Printf("CBP country code %s", record.CBPCountryCode.String)
				}
				return err
			}
			added++
		}
	}

	if err = fc.Db.UpdateFileStatus(ctx, fc.Tx, "IMPORTED", id, time.Now().UTC()); err != nil {
		return err
	}

	fc.Logger.Printf("completed processing '%s' file: loaded %d records", fc.Filename, added)
	return nil
}

func parseFC01Content(line string) (*FC01Record, error) {
	data := []rune(line)
	if len(data) < 1138 {
		return nil, errors.New("Incorrect line length: " + strconv.Itoa(len(data)))
	}

	// Application Number
	record := FC01Record{
		ApplicationNumber: strings.TrimSpace(util.SubstringBeginning(data, 11)),
	}

	// Type Of Method
	n, err := util.ReadInt64(strings.TrimSpace(util.Substring(data, 12, 21)))
	if err != nil {
		return nil, err
	}
	record.TypeOfMethod = n

	// Insured ID
	n, err = util.ReadInt64(strings.TrimSpace(util.Substring(data, 22, 31)))
	if err != nil {
		return nil, err
	}
	record.InsuredID = n

	// Agent Broker Number
	record.AgentBrokerNumber = strings.TrimSpace(strings.TrimSpace(util.Substring(data, 32, 37)))

	// Inspector Code
	record.InspectorCode = strings.TrimSpace(strings.TrimSpace(util.Substring(data, 38, 43)))
	// Not Used Creditor
	record.NotUsedCreditor = util.ReadNullString(strings.TrimSpace(util.Substring(data, 44, 49)))
	// User Update Name
	record.UserUpdateName = util.ReadNullString(strings.TrimSpace(util.Substring(data, 50, 62)))
	// User Code
	record.UserCode = util.ReadNullString(strings.TrimSpace(util.Substring(data, 63, 69)))

	// PM ID
	record.PMID, err = util.ReadNullInt64(strings.TrimSpace(util.Substring(data, 70, 80)))
	if err != nil {
		return nil, err
	}
	// Agent Name
	record.AgentName = util.ReadNullString(strings.TrimSpace(util.Substring(data, 81, 113)))
	// Agent Name
	record.BankName = util.ReadNullString(strings.TrimSpace(util.Substring(data, 114, 146)))
	// Agent Address 2
	record.AgentAddress2 = util.ReadNullString(strings.TrimSpace(util.Substring(data, 147, 179)))
	// Agent Address 3
	record.AgentAddress3 = util.ReadNullString(strings.TrimSpace(util.Substring(data, 180, 212)))
	// Agent Address 4
	record.AgentAddress4 = util.ReadNullString(strings.TrimSpace(util.Substring(data, 213, 245)))
	// Agent Address 5
	record.AgentAddress5 = util.ReadNullString(util.Substring(data, 246, 278))
	// Agent Post Code
	record.AgentPostCode = util.ReadNullString(util.Substring(data, 279, 284))
	// Agent Town
	record.AgentTown = util.ReadNullString(util.Substring(data, 285, 311))
	// Agent Country Code
	record.AgentCountryCode = util.ReadNullString(util.Substring(data, 312, 315))
	// Agent ID
	record.AgentID, err = util.ReadNullInt64(util.Substring(data, 316, 325))
	if err != nil {
		return nil, err
	}
	// Business Department Code
	record.BusinessDepartmentCode = util.ReadNullString(util.Substring(data, 326, 330))
	// Business Department Code For Invoice
	record.BusinessDepartmentCodeForInvoice = util.ReadNullString(util.Substring(data, 331, 335))
	// Application Last Updated Date
	record.ApplicationLastUpdatedDate, err = util.ReadDB2NullDate(util.Substring(data, 336, 346))
	if err != nil {
		return nil, err
	}
	// Application Notes
	record.ApplicationNotes = util.ReadNullString(util.Substring(data, 347, 575))
	// Contract Reference
	record.ContractReference = util.ReadNullString(util.Substring(data, 576, 591))
	// Applicant Reference
	record.ApplicantReference = util.ReadNullString(util.Substring(data, 592, 624))
	// Insured Address 1
	record.InsuredAddress1 = util.ReadNullString(util.Substring(data, 625, 657))
	// Insured Address 2
	record.InsuredAddress2 = util.ReadNullString(util.Substring(data, 658, 690))
	// Insured Address 3
	record.InsuredAddress3 = util.ReadNullString(util.Substring(data, 691, 723))
	// Insured Address 4
	record.InsuredAddress4 = util.ReadNullString(util.Substring(data, 724, 756))
	// Insured Address 5
	record.InsuredAddress5 = util.ReadNullString(util.Substring(data, 757, 789))
	// Insured Post Code
	record.InsuredPostCode = util.ReadNullString(util.Substring(data, 790, 795))
	// Insured Town
	record.InsuredTown = util.ReadNullString(util.Substring(data, 796, 822))
	// Insured Country
	record.InsuredCountry = util.ReadNullString(util.Substring(data, 823, 826))
	// Third Party Full Name
	record.ThirdPartyFullName = util.ReadNullString(util.Substring(data, 827, 859))
	// Third Party Address 2
	record.ThirdPartyAddress2 = util.ReadNullString(util.Substring(data, 860, 892))
	// Third Party Address 3
	record.ThirdPartyAddress3 = util.ReadNullString(util.Substring(data, 893, 925))
	// Third Party Address 4
	record.ThirdPartyAddress4 = util.ReadNullString(util.Substring(data, 926, 958))
	// Third Party Address 5
	record.ThirdPartyAddress5 = util.ReadNullString(util.Substring(data, 959, 991))
	// Third Party Post Code
	record.ThirdPartyPostCode = util.ReadNullString(util.Substring(data, 992, 997))
	// Third Party Town
	record.ThirdPartyTown = util.ReadNullString(util.Substring(data, 998, 1024))
	// Third Party Country Code
	record.ThirdPartyCountryCode = util.ReadNullString(util.Substring(data, 1025, 1028))
	// Decision Application Level
	record.DecisionApplicationLevel = util.ReadNullString(util.Substring(data, 1029, 1033))
	// Decision Created At
	record.DecisionCreatedAt, err = util.ReadMacaoNullTimestamp(util.Substring(data, 1034, 1060))
	if err != nil {
		return nil, err
	}
	// Decision Updated At
	record.DecisionUpdatedAt, err = util.ReadMacaoNullTimestamp(util.Substring(data, 1061, 1087))
	if err != nil {
		return nil, err
	}
	// CFF Editor Number
	record.CFFEditorNumber = util.ReadNullString(util.Substring(data, 1088, 1093))
	// Lender EMail
	record.LenderEmail = util.ReadNullString(util.Substring(data, 1094, 1137))
	// CBP Country Code
	record.CBPCountryCode = util.ReadNullString(util.SubstringEnd(data, 1138))
	return &record, nil
}

func addFC01Record(ctx context.Context, enc encryption.Client, tx *sql.Tx, fileId uint, record *FC01Record, addedAt time.Time) error {
	var id uint
	var err error

	if record.ApplicationNotes.Valid {
		record.ApplicationNotes.String, err = enc.Encrypt(record.ApplicationNotes.String)
		if err != nil {
			return err
		}
	}
	if record.InsuredAddress1.Valid {
		record.InsuredAddress1.String, err = enc.Encrypt(record.InsuredAddress1.String)
		if err != nil {
			return err
		}
	}
	if record.InsuredAddress2.Valid {
		record.InsuredAddress2.String, err = enc.Encrypt(record.InsuredAddress2.String)
		if err != nil {
			return err
		}
	}
	if record.InsuredAddress3.Valid {
		record.InsuredAddress3.String, err = enc.Encrypt(record.InsuredAddress3.String)
		if err != nil {
			return err
		}
	}
	if record.InsuredAddress4.Valid {
		record.InsuredAddress4.String, err = enc.Encrypt(record.InsuredAddress4.String)
		if err != nil {
			return err
		}
	}
	if record.InsuredAddress5.Valid {
		record.InsuredAddress5.String, err = enc.Encrypt(record.InsuredAddress5.String)
		if err != nil {
			return err
		}
	}
	if record.InsuredTown.Valid {
		record.InsuredTown.String, err = enc.Encrypt(record.InsuredTown.String)
		if err != nil {
			return err
		}
	}
	if record.ThirdPartyFullName.Valid {
		record.ThirdPartyFullName.String, err = enc.Encrypt(record.ThirdPartyFullName.String)
		if err != nil {
			return err
		}
	}
	if record.ThirdPartyAddress2.Valid {
		record.ThirdPartyAddress2.String, err = enc.Encrypt(record.ThirdPartyAddress2.String)
		if err != nil {
			return err
		}
	}
	if record.ThirdPartyAddress3.Valid {
		record.ThirdPartyAddress3.String, err = enc.Encrypt(record.ThirdPartyAddress3.String)
		if err != nil {
			return err
		}
	}
	if record.ThirdPartyAddress4.Valid {
		record.ThirdPartyAddress4.String, err = enc.Encrypt(record.ThirdPartyAddress4.String)
		if err != nil {
			return err
		}
	}
	if record.ThirdPartyAddress5.Valid {
		record.ThirdPartyAddress5.String, err = enc.Encrypt(record.ThirdPartyAddress5.String)
		if err != nil {
			return err
		}
	}
	if record.ThirdPartyTown.Valid {
		record.ThirdPartyTown.String, err = enc.Encrypt(record.ThirdPartyTown.String)
		if err != nil {
			return err
		}
	}

	err = tx.QueryRowContext(ctx, "INSERT INTO FC01_RECORDS (ID, FILE_ID, APPLICATION_NUMBER, TYPE_OF_METHOD, "+
		"INSURED_ID, AGENT_BROKER_NUMBER, INSPECTOR_CODE, NOT_USED_CREDITOR, USER_UPDATE_NAME, USER_CODE, PM_ID, "+
		"AGENT_NAME, BANK_NAME, AGENT_ADDRESS_2, AGENT_ADDRESS_3, AGENT_ADDRESS_4, AGENT_ADDRESS_5, AGENT_POST_CODE, "+
		"AGENT_TOWN, AGENT_COUNTRY, AGENT_ID, BUSINESS_DEPARTMENT_CODE, BUSINESS_DEPARTMENT_CODE_FOR_INVOICE, "+
		"APPLICATION_LAST_UPDATED_DATE, APPLICATION_NOTES, CONTRACT_REFERENCE, APPLICANT_REFERENCE, INSURED_ADDRESS_1, "+
		"INSURED_ADDRESS_2, INSURED_ADDRESS_3, INSURED_ADDRESS_4, INSURED_ADDRESS_5, INSURED_POST_CODE, "+
		"INSURED_TOWN, INSURED_COUNTRY_CODE, THIRD_PARTY_FULL_NAME, THIRD_PARTY_ADDRESS_2, THIRD_PARTY_ADDRESS_3, "+
		"THIRD_PARTY_ADDRESS_4, THIRD_PARTY_ADDRESS_5, THIRD_PARTY_POST_CODE, THIRD_PARTY_TOWN, THIRD_PARTY_COUNTRY_CODE, "+
		"DECISION_APPLICATION_LEVEL, DECISION_CREATED_AT, DECISION_UPDATED_AT, CFF_EDITOR_NUMBER, LENDER_EMAIL, "+
		"CBP_COUNTRY_CODE, CREATED_AT) VALUES (NEXTVAL('FC01_SEQ'), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, "+
		"$11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, "+
		"$31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49) RETURNING ID",
		fileId, record.ApplicationNumber, record.TypeOfMethod, record.InsuredID, record.AgentBrokerNumber, record.InspectorCode,
		record.NotUsedCreditor, record.UserUpdateName, record.UserCode, record.PMID, record.AgentName, record.BankName,
		record.AgentAddress2, record.AgentAddress3, record.AgentAddress4, record.AgentAddress5, record.AgentPostCode,
		record.AgentTown, record.AgentCountryCode, record.AgentID, record.BusinessDepartmentCode, record.BusinessDepartmentCodeForInvoice,
		record.ApplicationLastUpdatedDate, record.ApplicationNotes, record.ContractReference, record.ApplicantReference,
		record.InsuredAddress1, record.InsuredAddress2, record.InsuredAddress3, record.InsuredAddress4, record.InsuredAddress5,
		record.InsuredPostCode, record.InsuredTown, record.InsuredCountry, record.ThirdPartyFullName, record.ThirdPartyAddress2,
		record.ThirdPartyAddress3, record.ThirdPartyAddress4, record.ThirdPartyAddress5, record.ThirdPartyPostCode,
		record.ThirdPartyTown, record.ThirdPartyCountryCode, record.DecisionApplicationLevel, record.DecisionCreatedAt,
		record.DecisionUpdatedAt, record.CFFEditorNumber, record.LenderEmail, record.CBPCountryCode, addedAt).Scan(&id)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO FC01_RECORDS_HISTORY (ID, MODIFIED_AT, STATUS) VALUES ($1, $2, 'ADDED')", id, addedAt)
	if err != nil {
		return err
	}

	return nil
}