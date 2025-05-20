package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "embed"

	_ "github.com/go-sql-driver/mysql"
)

//go:embed create_customers.sql
var createCustomersTableSQL string

//go:embed create_patients.sql
var createPatientsTableSQL string

//go:embed create_old_index.sql
var createOldIndexTableSQL string

//go:embed create_split_index.sql
var createSplitIndexTableSQL string

//go:embed create_split_unified_index.sql
var createSplitUnifiedIndexTableSQL string

//go:embed create_automerge.sql
var createAutomergeTableSQL string

//go:embed select_size.sql
var tableSizeQuery string

//go:embed random_names_1million.csv
var millionRandomNames []byte
var namesReader = bytes.NewReader(millionRandomNames)

var (
	key1 = []byte("qhmQvonoeuGa_FAKE_KEY_1_lVtm6fLq")
	key2 = []byte("blindindexGa_FAKE_KEY_2_lVtm6fLq")
	key3 = []byte("iaminvalidat_FAKE_KEY_3_lVtm6fLq")
	key4 = []byte("testtestBJGa_FAKE_KEY_4_lVtm6fLq")
	key5 = []byte("qhmQsometext_FAKE_KEY_5_lVtm6fLq")
	key6 = []byte("qthisisaBJGa_FAKE_KEY_6_lVtm6fLq")
)

func createHmac(plain string, key []byte) byte {
	plain = strings.ToLower(plain)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(plain))
	hash := mac.Sum(nil)
	return byte(hash[1] + 1)
}

func truncLetterCount(letterCount int) int {
	return max(0, min(letterCount, 6))
}

func bitPattern(letterCount int) uint32 {
	pattern := uint32(0)
	for range bitCount(truncLetterCount(letterCount)) {
		pattern = (pattern << 1) | 0x01
	}
	return uint32(pattern)
}

func bitCount(letterCount int) uint32 {
	switch truncLetterCount(letterCount) {
	case 1:
		return 7
	case 2:
		return 6
	case 3:
		return 5
	case 4:
		return 2
	case 5:
		return 2
	case 6:
		return 2
	}
	return 0
}

func bitShift(letterCount int) uint32 {
	shift := uint32(0)
	for i := range truncLetterCount(letterCount) {
		shift += bitCount(i)
	}
	return uint32(shift)
}

func combineHmacs(letters1 byte, letters2 byte, letters3 byte, letters4 byte, letters5 byte, letters6 byte) uint32 {
	return uint32(
		((uint32(letters1)&bitPattern(1))<<bitShift(1))|
			((uint32(letters2)&bitPattern(2))<<bitShift(2))|
			((uint32(letters3)&bitPattern(3))<<bitShift(3))|
			((uint32(letters4)&bitPattern(4))<<bitShift(4))|
			((uint32(letters5)&bitPattern(5))<<bitShift(5))|
			((uint32(letters6)&bitPattern(6))<<bitShift(6)),
	) & 0xffffffff
}

func getKey(index int) []byte {
	index = min(index, 6)
	switch index {
	case 1:
		return key1
	case 2:
		return key2
	case 3:
		return key3
	case 4:
		return key4
	case 5:
		return key5
	case 6:
		return key6
	}
	return key6
}

func zeroOrHmac(word string, index int) byte {
	if len(word) < index {
		return 0
	}
	return createHmac(word[:index], getKey(index))
}

func getBidxHmac(word string) uint32 {
	return combineHmacs(zeroOrHmac(word, 1), zeroOrHmac(word, 2), zeroOrHmac(word, 3), zeroOrHmac(word, 4), zeroOrHmac(word, 5), zeroOrHmac(word, 6))
}

func bitMask(letterCount int) uint32 {
	bitMask := uint32(0)
	for i := range truncLetterCount(letterCount) {
		bitMask = bitMask | bitPattern(i+1)<<bitShift(i+1)
	}
	return bitMask
}

type UserName struct {
	FirstName string `json:"first"`
	LastName  string `json:"last"`
}
type User struct {
	Name UserName `json:"name"`
}
type GetUsersResponse struct {
	Error   string `json:"error"`
	Results []User `json:"results"`
}

var lck = sync.Mutex{}

func getRandomUsers(amount int, _ string) ([]User, error) {
	lck.Lock()
	defer lck.Unlock()
	reader := csv.NewReader(namesReader)
	reader.FieldsPerRecord = 2
	users := make([]User, amount)
	for i := range amount {
		record, err := reader.Read()
		if err != nil {
			return nil, err
		}
		users[i].Name = UserName{FirstName: record[0], LastName: record[1]}
	}
	return users, nil
}

func doChunked[T any](values []T, chunkSize int, f func(startIndex int, chunk []T) error) {
	total := len(values)
	valuesChunked := [][]T{}
	for i := range total / chunkSize {
		valuesChunked = append(valuesChunked, values[i*chunkSize:min((i+1)*chunkSize, total)])
	}
	for i, chunk := range valuesChunked {
		log.Printf("Handling chunk %d/%d\n", i+1, len(valuesChunked))
		err := f(i*chunkSize, chunk)
		if err != nil {
			panic(err)
		}
		log.Printf("Done with %d/%d\n", min((i+1)*chunkSize, total), total)
	}
}

func main() {
	var command string
	ncids := 10
	flag.StringVar(&command, "command", "search", "enter 'search', 'populate', 'stats', or 'prep'")
	var search string
	flag.StringVar(&search, "search", "", "enter text to search if searching")
	flag.Parse()

	db, err := sql.Open("mysql", "root:password@tcp(127.0.0.1:3306)/db")
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()
	fmt.Println(command)
	switch command {
	case "stats":
		res, err := db.Query(tableSizeQuery)
		if err != nil {
			panic(err)
		}
		tableSizes := []float64{}
		tableNames := []string{}
		for res.Next() {
			var tbl string
			var indexSizeMB float64
			err = res.Scan(&tbl, &indexSizeMB)
			if err != nil {
				panic(err)
			}
			tableNames = append(tableNames, tbl)
			tableSizes = append(tableSizes, indexSizeMB)
		}

		totalSize := 0.0
		for i, name := range tableNames {
			fmt.Printf("Table: %s\n\t  Size: %f MB\n\t", name, tableSizes[i])
			totalSize += tableSizes[i]
		}
		fmt.Printf("---------------------------\n\t")
		fmt.Printf("Total Size:          %f MB\n\t", totalSize)

		res, err = db.Query("SELECT COUNT(*) FROM patients;")
		if err != nil {
			panic(err)
		}
		for res.Next() {
			var rows int
			err = res.Scan(&rows)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Total patients: %d\n", rows)
		}
	case "search":
		fmt.Printf("search parameters for %q\n", search)
		hmac := getBidxHmac(search)
		fmt.Printf("Bit Mask as decimal: %d\n", bitMask(len(search)))
		fmt.Printf("Blind Index HMAC for %s (%d letters): %d (0x%x)\n", search, min(len(search), 6), hmac, hmac)

	case "prep":
		// drop by default
		log.Println("started")
		_, err = db.Exec("drop table if exists patients_automerge;")
		if err != nil {
			panic(err)
		}

		_, err = db.Exec("drop table if exists patients_bidx;")
		if err != nil {
			panic(err)
		}
		log.Println("dropped patients_bidx")

		_, err = db.Exec("drop table if exists patients_bidx_split;")
		if err != nil {
			panic(err)
		}
		log.Println("dropped patients_bidx_split")

		_, err = db.Exec("drop table if exists patients_bidx_split_unified;")
		if err != nil {
			panic(err)
		}
		log.Println("dropped patients_bidx_split_unified")

		_, err = db.Exec("drop table if exists patients;")
		if err != nil {
			panic(err)
		}
		log.Println("dropped patients")
		_, err = db.Exec("drop table if exists customers;")
		if err != nil {
			panic(err)
		}
		log.Println("dropped customers")

		// populate db
		_, err = db.Exec(createCustomersTableSQL)
		log.Println("created customers table")
		if err != nil {
			panic(err)
		}
		for i := range ncids {
			_, err = db.Exec("insert into customers (id) values (?);", i+1)
			if err != nil {
				panic(err)
			}
		}
		log.Println("created customers")

		_, err = db.Exec(createPatientsTableSQL)
		if err != nil {
			panic(err)
		}
		log.Println("created patients")

		_, err = db.Exec(createOldIndexTableSQL)
		if err != nil {
			panic(err)
		}
		log.Println("created old index")

		_, err = db.Exec(createSplitIndexTableSQL)
		if err != nil {
			panic(err)
		}
		log.Println("created split index")

		_, err = db.Exec(createSplitUnifiedIndexTableSQL)
		if err != nil {
			panic(err)
		}
		log.Println("created split unified index")
		_, err = db.Exec(createAutomergeTableSQL)
		if err != nil {
			panic(err)
		}
		log.Println("created automerge")
	case "populate":
		usersPerCount := 1000000
		startOffset := 1000000
		randomUsers, err := getRandomUsers(usersPerCount, "de")
		if err != nil {
			panic(err)
		}
		fmt.Printf("got %d random users...\n", len(randomUsers))
		doChunked(randomUsers, 100000, func(startIndex int, randomUsers []User) error {
			startIndex = startIndex + startOffset
			f, err := os.OpenFile(fmt.Sprintf("./setup_%d.sql", startIndex), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()
			if err != nil {
				panic(err)
			}
			log.Println("creating insert statements")
			_, _ = fmt.Fprint(f, `insert into patients (
				cid,
				emr_pid,
				dob,
				email,
				first_name,
				last_name,
				bidx_fn,
				bidx_ln
				)
				values `)
			for i_pre_add, user := range randomUsers {
				i := i_pre_add + startIndex + 1

				patient_emr_pid := fmt.Sprintf("emr_id_%d", i)
				patient_cid := (i % ncids) + 1 // n customers, spread the data out
				patient_email := fmt.Sprintf("%s.%s@mail.test", user.Name.FirstName, user.Name.LastName)
				patient_dob := time.Now().UTC().Add(-time.Hour * 24 * time.Duration(i%365)).Format("2006-01-02")
				lowerLastNamePadded := strings.ToLower(user.Name.LastName) + "        "
				lowerFirstNamePadded := strings.ToLower(user.Name.FirstName) + "        "
				fnhmac := getBidxHmac(lowerFirstNamePadded)
				lnhmac := getBidxHmac(lowerLastNamePadded)
				_, _ = fmt.Fprintf(f, `(%d,%q,%q,%q,%q,%q,%d,%d)`,
					patient_cid,
					patient_emr_pid,
					patient_dob,
					patient_email,
					user.Name.FirstName,
					user.Name.LastName,
					fnhmac,
					lnhmac,
				)
				if i < len(randomUsers)+startIndex {
					_, _ = fmt.Fprint(f, ",\n")
				}
			}
			log.Println("patient insert statements created")
			_, _ = fmt.Fprint(f, ";\n")

			_, _ = fmt.Fprint(f, `insert into patients_bidx (
				cid,
				pid,
				lnidx,
				fnidx
				)
				values `)
			for i_pre_add, user := range randomUsers {
				i := i_pre_add + startIndex + 1

				patient_cid := (i % ncids) + 1 // n customers, spread the data out
				lowerLastNamePadded := strings.ToLower(user.Name.LastName) + "        "
				lowerFirstNamePadded := strings.ToLower(user.Name.FirstName) + "        "
				fnhmac := getBidxHmac(lowerFirstNamePadded)
				lnhmac := getBidxHmac(lowerLastNamePadded)
				_, _ = fmt.Fprintf(f, `(%d,%d,%d,%d)`,
					patient_cid,
					i,
					lnhmac,
					fnhmac,
				)
				if i < len(randomUsers)+startIndex {
					_, _ = fmt.Fprint(f, ",\n")
				}
			}
			log.Println("patient_bidx insert statements created")
			_, _ = fmt.Fprint(f, ";\n")

			_, _ = fmt.Fprint(f, `insert into patients_bidx_split (
				pid,
				cid,
				idx_fn_one,
				idx_fn_two,
				idx_fn_three,
				idx_fn_four,
				idx_fn_five,
				idx_fn_six,
				idx_ln_one,
				idx_ln_two,
				idx_ln_three,
				idx_ln_four,
				idx_ln_five,
				idx_ln_six
				)
				values `)
			for i_pre_add, user := range randomUsers {
				i := i_pre_add + startIndex + 1

				patient_cid := (i % ncids) + 1 // n customers, spread the data out
				lowerLastNamePadded := strings.ToLower(user.Name.LastName) + "        "
				lowerFirstNamePadded := strings.ToLower(user.Name.FirstName) + "        "
				parts_fn_1 := zeroOrHmac(lowerFirstNamePadded, 1)
				parts_fn_2 := zeroOrHmac(lowerFirstNamePadded, 2)
				parts_fn_3 := zeroOrHmac(lowerFirstNamePadded, 3)
				parts_fn_4 := zeroOrHmac(lowerFirstNamePadded, 4)
				parts_fn_5 := zeroOrHmac(lowerFirstNamePadded, 5)
				parts_fn_6 := zeroOrHmac(lowerFirstNamePadded, 6)
				parts_ln_1 := zeroOrHmac(lowerLastNamePadded, 1)
				parts_ln_2 := zeroOrHmac(lowerLastNamePadded, 2)
				parts_ln_3 := zeroOrHmac(lowerLastNamePadded, 3)
				parts_ln_4 := zeroOrHmac(lowerLastNamePadded, 4)
				parts_ln_5 := zeroOrHmac(lowerLastNamePadded, 5)
				parts_ln_6 := zeroOrHmac(lowerLastNamePadded, 6)
				fnhmac_1 := combineHmacs(parts_fn_1, 0, 0, 0, 0, 0)
				fnhmac_2 := combineHmacs(parts_fn_1, parts_fn_2, 0, 0, 0, 0)
				fnhmac_3 := combineHmacs(parts_fn_1, parts_fn_2, parts_fn_3, 0, 0, 0)
				fnhmac_4 := combineHmacs(parts_fn_1, parts_fn_2, parts_fn_3, parts_fn_4, 0, 0)
				fnhmac_5 := combineHmacs(parts_fn_1, parts_fn_2, parts_fn_3, parts_fn_4, parts_fn_5, 0)
				fnhmac_6 := combineHmacs(parts_fn_1, parts_fn_2, parts_fn_3, parts_fn_4, parts_fn_5, parts_fn_6)
				lnhmac_1 := combineHmacs(parts_ln_1, 0, 0, 0, 0, 0)
				lnhmac_2 := combineHmacs(parts_ln_1, parts_ln_2, 0, 0, 0, 0)
				lnhmac_3 := combineHmacs(parts_ln_1, parts_ln_2, parts_ln_3, 0, 0, 0)
				lnhmac_4 := combineHmacs(parts_ln_1, parts_ln_2, parts_ln_3, parts_ln_4, 0, 0)
				lnhmac_5 := combineHmacs(parts_ln_1, parts_ln_2, parts_ln_3, parts_ln_4, parts_ln_5, 0)
				lnhmac_6 := combineHmacs(parts_ln_1, parts_ln_2, parts_ln_3, parts_ln_4, parts_ln_5, parts_ln_6)
				_, _ = fmt.Fprintf(f, `(%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)`,
					i,
					patient_cid,
					fnhmac_1,
					fnhmac_2,
					fnhmac_3,
					fnhmac_4,
					fnhmac_5,
					fnhmac_6,
					lnhmac_1,
					lnhmac_2,
					lnhmac_3,
					lnhmac_4,
					lnhmac_5,
					lnhmac_6,
				)
				if i < len(randomUsers)+startIndex {
					_, _ = fmt.Fprint(f, ",\n")
				}
			}
			log.Println("patient_bidx_split insert statements created")
			_, _ = fmt.Fprint(f, ";\n")

			_, _ = fmt.Fprint(f, `insert into patients_bidx_split_unified (
				pid,
				cid,
				idx_one,
				idx_two,
				idx_three,
				idx_four,
				idx_five,
				idx_six
				)
				values `)
			for i_pre_add, user := range randomUsers {
				i := i_pre_add + startIndex + 1

				patient_cid := (i % ncids) + 1 // n customers, spread the data out
				lowerLastNamePadded := strings.ToLower(user.Name.LastName) + "        "
				lowerFirstNamePadded := strings.ToLower(user.Name.FirstName) + "        "
				parts_fn_1 := zeroOrHmac(lowerFirstNamePadded, 1)
				parts_fn_2 := zeroOrHmac(lowerFirstNamePadded, 2)
				parts_fn_3 := zeroOrHmac(lowerFirstNamePadded, 3)
				parts_fn_4 := zeroOrHmac(lowerFirstNamePadded, 4)
				parts_fn_5 := zeroOrHmac(lowerFirstNamePadded, 5)
				parts_fn_6 := zeroOrHmac(lowerFirstNamePadded, 6)
				parts_ln_1 := zeroOrHmac(lowerLastNamePadded, 1)
				parts_ln_2 := zeroOrHmac(lowerLastNamePadded, 2)
				parts_ln_3 := zeroOrHmac(lowerLastNamePadded, 3)
				parts_ln_4 := zeroOrHmac(lowerLastNamePadded, 4)
				parts_ln_5 := zeroOrHmac(lowerLastNamePadded, 5)
				parts_ln_6 := zeroOrHmac(lowerLastNamePadded, 6)
				fnhmac_1 := combineHmacs(parts_fn_1, 0, 0, 0, 0, 0)
				fnhmac_2 := combineHmacs(parts_fn_1, parts_fn_2, 0, 0, 0, 0)
				fnhmac_3 := combineHmacs(parts_fn_1, parts_fn_2, parts_fn_3, 0, 0, 0)
				fnhmac_4 := combineHmacs(parts_fn_1, parts_fn_2, parts_fn_3, parts_fn_4, 0, 0)
				fnhmac_5 := combineHmacs(parts_fn_1, parts_fn_2, parts_fn_3, parts_fn_4, parts_fn_5, 0)
				fnhmac_6 := combineHmacs(parts_fn_1, parts_fn_2, parts_fn_3, parts_fn_4, parts_fn_5, parts_fn_6)
				lnhmac_1 := combineHmacs(parts_ln_1, 0, 0, 0, 0, 0)
				lnhmac_2 := combineHmacs(parts_ln_1, parts_ln_2, 0, 0, 0, 0)
				lnhmac_3 := combineHmacs(parts_ln_1, parts_ln_2, parts_ln_3, 0, 0, 0)
				lnhmac_4 := combineHmacs(parts_ln_1, parts_ln_2, parts_ln_3, parts_ln_4, 0, 0)
				lnhmac_5 := combineHmacs(parts_ln_1, parts_ln_2, parts_ln_3, parts_ln_4, parts_ln_5, 0)
				lnhmac_6 := combineHmacs(parts_ln_1, parts_ln_2, parts_ln_3, parts_ln_4, parts_ln_5, parts_ln_6)
				_, _ = fmt.Fprintf(f, `(%d,%d,%d,%d,%d,%d,%d,%d),(%d,%d,%d,%d,%d,%d,%d,%d)`,
					i,
					patient_cid,
					fnhmac_1,
					fnhmac_2,
					fnhmac_3,
					fnhmac_4,
					fnhmac_5,
					fnhmac_6,
					i,
					patient_cid,
					lnhmac_1,
					lnhmac_2,
					lnhmac_3,
					lnhmac_4,
					lnhmac_5,
					lnhmac_6,
				)
				if i < len(randomUsers)+startIndex {
					_, _ = fmt.Fprint(f, ",\n")
				}
			}
			_, _ = fmt.Fprint(f, ";\n")
			log.Println("patient_bidx_split_unified insert statements created")
			_, _ = fmt.Fprint(f, `insert into patients_automerge ( pid, cid, automerge_full_name_dob, automerge_email_dob) values `)
			for i_pre_add, user := range randomUsers {
				i := i_pre_add + startIndex + 1
				patient_cid := (i % ncids) + 1 // n customers, spread the data out
				patient_email := fmt.Sprintf("%s.%s@mail.test", user.Name.FirstName, user.Name.LastName)
				patient_dob := time.Now().UTC().Add(-time.Hour * 24 * time.Duration(i%365)).Format("2006-01-02")

				hashName := sha256.New().Sum([]byte(user.Name.FirstName + user.Name.LastName + patient_dob))
				hashEmail := sha256.New().Sum([]byte(patient_email + patient_dob))

				_, _ = fmt.Fprintf(f, `(%d,%d,X'%s',X'%s')`, i, patient_cid, hex.EncodeToString(hashName[:32]), hex.EncodeToString(hashEmail[:32]))
				if i < len(randomUsers)+startIndex {
					_, _ = fmt.Fprint(f, ",\n")
				}
			}
			log.Println("automerge insert statements created")
			_, _ = fmt.Fprint(f, ";\n")
			return nil
		})

	}
}
