package fc06

import (
	"bufio"
	"context"
	"database/sql"
	"github.axa.com/axa-partners-clp/mrt-shared/encryption"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/util"
	"github.com/spf13/viper"
	"syreclabs.com/go/faker"
	"time"
)

type File struct {
	*inputfile.SourceFile
}

// Source table name: KJDTADR
type FC06Record struct {
	InsuredID           int64          // line[0]  (IDGMAS)
	InsuredRole         sql.NullString // line[1]  (CYASRL)
	AddressCreationDate sql.NullTime   // line[2]  (DDCHTA)
	AddressLine1        sql.NullString // line[3]  (LNGMA1) - encrypt, anonymize
	AddressLine2        sql.NullString // line[4]  (LRGMA2) - encrypt, anonymize
	AddressLine3        sql.NullString // line[5]  (LRGMA3) - encrypt, anonymize
	AddressLine4        sql.NullString // line[6]  (LRGMA4) - encrypt, anonymize
	AddressLine5        sql.NullString // line[7]  (LRGMA5) - encrypt, anonymize
	PostCode            sql.NullString // line[8]  (CPGMA6)
	Town                sql.NullString // line[9]  (LVGMA6) - encrypt, anonymize
	CountryCode         sql.NullString // line[10] (CDPAPP)
	MobileNumber        sql.NullString // line[11] (TELPOR) - encrypt, anonymize
	PhoneNumber         sql.NullString // line[12] (TELFIX) - encrypt, anonymize
	Email               sql.NullString // line[13] (ADMAIL) - encrypt, anonymize
}

func (fc File) AsyncProcessFile(ctx context.Context) {
	fc.Process(ctx, fc.ProcessFile)
}

func (fc File) ProcessFile(ctx context.Context) error {

	addedAt := time.Now().UTC()
	id, err := fc.Db.AddFile(ctx, fc.Tx, fc.BatchID, fc.Filename, "FC06", addedAt)

	r := bufio.NewScanner(fc.Reader)
	// Skip the first line
	r.Scan()

	count := 0
	added := 0
	for r.Scan() {
		count++
		line := r.Text()
		insuredId, err := util.ReadInt64(util.SubstringBeginning([]rune(line), 9))
		if err != nil {
			fc.Logger.Printf("cannot parse the insuredId: %v", err)
		}

		if len(line) < 308 {
			fc.Logger.Printf("line %d: insured Id '%d' has incorrect line length %d", count, insuredId, len(line))
			continue
		}

		if fc.Range.InsuredIdExists(insuredId) {
			record, err := parseFC06Content(line)
			if err != nil {
				fc.Logger.Printf("cannot parse F06 line %d (%s): %v", count, line, err)
				return err
			}

			err = addFC06Record(ctx, fc.Encryption, fc.Tx, id, record, addedAt)
			if err != nil {
				fc.Logger.Printf("cannot add F06 record to Database from line %d (%s): %v", count, line, err)
				return err
			}
			added++
		}
	}

	err = fc.Db.UpdateFileStatus(ctx, fc.Tx, "IMPORTED", id, time.Now().UTC())

	fc.Logger.Printf("completed processing '%s' file, loaded %d records", fc.Filename, added)
	return nil
}

func parseFC06Content(line string) (*FC06Record, error) {
	data := []rune(line)

	// Insured ID
	n, err := util.ReadInt64(util.SubstringBeginning(data, 9))
	if err != nil {
		return nil, err
	}
	record := FC06Record{
		InsuredID: n,
	}
	// Insured Role
	record.InsuredRole = util.ReadNullString(util.Substring(data, 10, 13))
	// Decision Created At
	record.AddressCreationDate, err = util.ReadMacaoNullTimestamp(util.Substring(data, 14, 40))
	if err != nil {
		return nil, err
	}
	// Address Line 1
	record.AddressLine1 = util.ReadNullString(util.Substring(data, 41, 73))
	// Address Line 2
	record.AddressLine2 = util.ReadNullString(util.Substring(data, 74, 106))
	// Address Line 3
	record.AddressLine3 = util.ReadNullString(util.Substring(data, 107, 139))
	// Address Line 4
	record.AddressLine4 = util.ReadNullString(util.Substring(data, 140, 172))
	// Address Line 5
	record.AddressLine5 = util.ReadNullString(util.Substring(data, 173, 205))
	// Post Code
	record.PostCode = util.ReadNullString(util.Substring(data, 206, 211))
	// Town
	record.Town = util.ReadNullString(util.Substring(data, 212, 238))
	// Country Code
	record.CountryCode = util.ReadNullString(util.Substring(data, 239, 242))
	// Mobile Number
	record.MobileNumber = util.ReadNullString(util.Substring(data, 243, 253))
	// Phone Number
	record.PhoneNumber = util.ReadNullString(util.Substring(data, 254, 264))
	// Email
	record.Email = util.ReadNullString(util.SubstringEnd(data, 265))

	if viper.GetBool("anonymize") {
		if record.AddressLine1.Valid || record.AddressLine2.Valid || record.AddressLine3.Valid || record.AddressLine4.Valid || record.AddressLine5.Valid {
			record.AddressLine1.Valid = true
			record.AddressLine1.String = faker.Address().StreetAddress()
			record.AddressLine2.Valid = false
			record.AddressLine2.String = ""
			record.AddressLine3.Valid = false
			record.AddressLine3.String = ""
			record.AddressLine4.Valid = false
			record.AddressLine4.String = ""
			record.AddressLine5.Valid = false
			record.AddressLine5.String = ""
		}

		if record.Town.Valid {
			record.Town.String = faker.Address().City()
		}

		if record.MobileNumber.Valid {
			record.MobileNumber.String = faker.PhoneNumber().CellPhone()
		}

		if record.PhoneNumber.Valid {
			record.PhoneNumber.String = faker.PhoneNumber().PhoneNumber()
		}

		if record.Email.Valid {
			record.Email.String = faker.Internet().Email()
		}
	}

	return &record, nil
}

func addFC06Record(ctx context.Context, enc encryption.Client, tx *sql.Tx, fileId uint, record *FC06Record, addedAt time.Time) error {
	var id uint
	var err error

	if record.AddressLine1.Valid {
		record.AddressLine1.String, err = enc.Encrypt(record.AddressLine1.String)

	}

	if record.AddressLine2.Valid {
		record.AddressLine2.String, err = enc.Encrypt(record.AddressLine2.String)

	}

	if record.AddressLine3.Valid {
		record.AddressLine3.String, err = enc.Encrypt(record.AddressLine3.String)

	}

	if record.AddressLine4.Valid {
		record.AddressLine4.String, err = enc.Encrypt(record.AddressLine4.String)

	}

	if record.AddressLine5.Valid {
		record.AddressLine5.String, err = enc.Encrypt(record.AddressLine5.String)

	}

	if record.Town.Valid {
		record.Town.String, err = enc.Encrypt(record.Town.String)

	}

	if record.MobileNumber.Valid {
		record.MobileNumber.String, err = enc.Encrypt(record.MobileNumber.String)

	}

	if record.PhoneNumber.Valid {
		record.PhoneNumber.String, err = enc.Encrypt(record.PhoneNumber.String)

	}

	if record.Email.Valid {
		record.Email.String, err = enc.Encrypt(record.Email.String)

	}

	err = tx.QueryRowContext(ctx, "INSERT INTO FC06_RECORDS (ID, FILE_ID, INSURED_ID, INSURED_ROLE, "+
		"ADDRESS_CREATION_DATE, ADDRESS_LINE_1, ADDRESS_LINE_2, ADDRESS_LINE_3, ADDRESS_LINE_4, ADDRESS_LINE_5, POST_CODE, "+
		"TOWN, COUNTRY_CODE, MOBILE_NUMBER, TELEPHONE_NUMBER, EMAIL, CREATED_AT) VALUES (NEXTVAL('FC06_SEQ'), $1, "+
		"$2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) RETURNING ID",
		fileId, record.InsuredID, record.InsuredRole, record.AddressCreationDate, record.AddressLine1, record.AddressLine2,
		record.AddressLine3, record.AddressLine4, record.AddressLine5, record.PostCode, record.Town, record.CountryCode,
		record.MobileNumber, record.PhoneNumber, record.Email, addedAt).Scan(&id)

	return nil
}