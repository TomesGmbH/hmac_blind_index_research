package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"slices"
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

//go:embed create_fn_index.sql
var createFnIndexTableSQL string

//go:embed create_ln_index.sql
var createLnIndexTableSQL string

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

func cumulativeBitCount(letterCount int) int {
	switch truncLetterCount(letterCount) {
	case 1:
		return 7
	case 2:
		return 7 + 6
	case 3:
		return 7 + 6 + 5
	case 4:
		return 7 + 6 + 5 + 2
	case 5:
		return 7 + 6 + 5 + 2 + 2
	case 6:
		return 7 + 6 + 5 + 2 + 2 + 2
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

func separateBits(num uint32, nBits int) ([]int, []int) {
	setBits := []int{}
	unsetBits := []int{}

	// Loop through all 32 bits
	for i := range min(nBits, 24) {
		// Check if the bit at position i is set to 1
		if (num & (1 << i)) != 0 {
			setBits = append(setBits, i)
		} else {
			unsetBits = append(unsetBits, i)
		}
	}

	return setBits, unsetBits
}

func getSearchData(word string) (ones []int, zeroes []int) {
	index := max(1, min(len(word), 6))
	hmac := getBidxHmac(word)
	nBits := cumulativeBitCount(index)
	// fmtString := fmt.Sprintf("hmac: 0x%%x (0b%%%db)\n", nBits)
	// fmt.Printf(fmtString, hmac, hmac)
	// fmt.Printf("nBits: %d\n", nBits)
	return separateBits(hmac, nBits)
}

func getSearchQuery(cid int, name_ones []int, name_zeroes []int) string {
	b := strings.Builder{}
	b.WriteString(`select 
		p.emr_pid, 
		p.first_name, 
		p.last_name
	from patients as p
	`)

	for i := range name_ones {
		fmt.Fprintf(&b, " INNER JOIN patients_fn_bidx as fbidx_one%d ON fbidx_one%d.pid = p.id AND fbidx_one%d.cid = p.cid ", i, i, i)
	}

	for i, z := range name_zeroes {
		fmt.Fprintf(&b, " LEFT JOIN patients_fn_bidx as fbidx_zero%d ON fbidx_zero%d.pid = p.id AND fbidx_zero%d.cid = p.cid AND fbidx_zero%d.ibit = %d ", i, i, i, i, z)
	}

	fmt.Fprintf(&b, ` WHERE p.cid = %d `, cid)

	for i, o := range name_ones {
		fmt.Fprintf(&b, " AND fbidx_one%d.ibit = %d ", i, o)
	}

	for i := range name_zeroes {
		fmt.Fprintf(&b, " AND fbidx_zero%d.pid is null ", i)
	}

	b.WriteString(` UNION ALL
	select 
		p.emr_pid, 
		p.first_name, 
		p.last_name
	from patients as p
	`)

	for i := range name_ones {
		fmt.Fprintf(&b, " INNER JOIN patients_ln_bidx as lbidx_one%d ON lbidx_one%d.pid = p.id AND lbidx_one%d.cid = p.cid ", i, i, i)
	}

	for i, z := range name_zeroes {
		fmt.Fprintf(&b, " LEFT JOIN patients_ln_bidx as lbidx_zero%d ON lbidx_zero%d.pid = p.id AND lbidx_zero%d.cid = p.cid AND lbidx_zero%d.ibit = %d ", i, i, i, i, z)
	}

	fmt.Fprintf(&b, ` WHERE p.cid = %d `, cid)

	for i, o := range name_ones {
		fmt.Fprintf(&b, " AND lbidx_one%d.ibit = %d ", i, o)
	}

	for i := range name_zeroes {
		fmt.Fprintf(&b, " AND lbidx_zero%d.pid is null ", i)
	}

	return b.String()
}

// separateBits takes a uint32 number and returns two slices:
// - A slice containing the positions of bits that are set to 1
// - A slice containing the positions of bits that are set to 0
// The bit positions are 0-indexed, with position 0 being the least significant bit

type SearchResult struct {
	emr_pid               string
	first_name, last_name string
	matching_letters_fn   int
	matching_letters_ln   int
	match                 int
	matchWordLen          int
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

func track(msg string) (string, time.Time) {
	return msg, time.Now()
}

func duration(msg string, start time.Time) {
	fmt.Printf("%v: %v\n", msg, time.Since(start))
}

func queryFor(db *sql.DB, search string) (*sql.Rows, error) {
	ones, zeroes := getSearchData(search)
	fmt.Printf("ones: %+v, zeroes: %+v\n", ones, zeroes)
	defer duration(track("query time:"))
	return db.Query(getSearchQuery(1, ones, zeroes))
}

func explainQueryFor(db *sql.DB, search string) (*sql.Rows, error) {
	ones, zeroes := getSearchData(search)
	fmt.Printf("ones: %+v, zeroes: %+v\n", ones, zeroes)
	defer duration(track("query time:"))
	return db.Query("EXPLAIN " + getSearchQuery(1, ones, zeroes))
}

var (
	searchLimit int
	explain     bool
)

func main() {
	queryPool := sync.Pool{
		New: func() interface{} {
			return &strings.Builder{}
		},
	}
	getBuilder := func() *strings.Builder {
		return queryPool.Get().(*strings.Builder)
	}
	returnBuilder := func(b *strings.Builder) {
		b.Reset()
		queryPool.Put(b)
	}
	var command string
	ncids := 83
	flag.StringVar(&command, "command", "search", "enter 'search', 'populate', 'stats', or 'prep'")
	var search string
	flag.StringVar(&search, "search", "", "enter text to search if searching")
	var count int
	flag.IntVar(&count, "count", 1, "enter loops of inserting")
	flag.IntVar(&searchLimit, "limit", 1000, "enter search limit")
	flag.BoolVar(&explain, "explain", false, "do explain")
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
		tableSizes := []int{}
		tableNames := []string{}
		for res.Next() {
			var tbl string
			var indexSizeBytes int
			err = res.Scan(&tbl, &indexSizeBytes)
			if err != nil {
				panic(err)
			}
			tableNames = append(tableNames, tbl)
			tableSizes = append(tableSizes, indexSizeBytes)
		}

		totalSize := 0
		for i, name := range tableNames {
			fmt.Printf("Table: %s\n\t  Size: %d MB\n\t", name, tableSizes[i])
			totalSize += tableSizes[i]
		}
		fmt.Printf("---------------------------\n\t")
		fmt.Printf("Total Size:          %d MB\n\t", totalSize)

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
		fmt.Printf("searching for %q\n", search)
		if explain {
			rows, err := explainQueryFor(db, search)
			if err != nil {
				panic(err)
			}

			defer func() { _ = rows.Close() }()
			for rows.Next() {
				// id select_type table partitions type possible_keys key key_len ref rows filtered Extra
				var id, key_len, nrows, filtered sql.NullInt64
				var selectType, table, partitions, etype, possible_keys, key, ref, extra sql.NullString
				err := rows.Scan(&id, &selectType, &table, &partitions, &etype, &possible_keys, &key, &key_len, &ref, &nrows, &filtered, &extra)
				if err != nil {
					panic(err)
				}
				fmt.Printf(`
- id: %d
	select_type:   %s,
	table:         %s
	partitions:    %s,
	type:          %s,
	possible_keys: %s,
	key:           %s,
	key_len:       %d,
	ref:           %s,
	rows:          %d,
	filtered:      %d,
	extra:         %s,
`, id.Int64, selectType.String, table.String, partitions.String, etype.String, possible_keys.String, key.String, key_len.Int64, ref.String, nrows.Int64, filtered.Int64, extra.String,
				)
			}
			return
		}
		rows, err := queryFor(db, search)
		if err != nil {
			panic(err)
		}
		values := []SearchResult{}
		valuesMatchingWholeSearch := 0
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			value := SearchResult{}
			err = rows.Scan(&value.emr_pid, &value.first_name, &value.last_name)
			if err != nil {
				panic(err)
			}
			firstNameLower := strings.ToLower(value.first_name)
			lastNameLower := strings.ToLower(value.last_name)
			searchLower := strings.ToLower(search)
			checkFirstName := true
			checkLastName := true
			value.matching_letters_fn = 0
			value.matching_letters_ln = 0
			for i := range searchLower {
				if checkFirstName && len(firstNameLower) > i {
					if firstNameLower[i] == searchLower[i] {
						value.matching_letters_fn++
					} else {
						checkFirstName = false
					}
				}
				if checkLastName && len(lastNameLower) > i {
					if lastNameLower[i] == searchLower[i] {
						value.matching_letters_ln++
					} else {
						checkLastName = false
					}
				}
			}
			if value.matching_letters_fn > value.matching_letters_ln {
				value.match = value.matching_letters_fn
				value.matchWordLen = len([]byte(value.first_name))
			} else {
				value.match = value.matching_letters_ln
				value.matchWordLen = len([]byte(value.last_name))
			}

			if value.match == len(search) {
				valuesMatchingWholeSearch++
			}
			values = append(values, value)
		}
		if err != nil {
			panic(err)
		}
		avgMatch := 0.0
		matchSum := 0
		avgMatchWordLen := 0.0
		matchWordLenSum := 0
		avgMatchWordLenDiff := 0.0
		matchWordLenDiffSum := 0
		for _, v := range values {
			matchSum += v.match
			matchWordLenSum += v.matchWordLen
			matchWordLenDiffSum += v.matchWordLen - v.match
		}
		avgMatch = float64(matchSum) / float64(len(values))
		avgMatchWordLen = float64(matchWordLenSum) / float64(len(values))
		avgMatchWordLenDiff = float64(matchWordLenDiffSum) / float64(len(values))
		topWordsSlice := slices.Clone(values)
		slices.SortFunc(topWordsSlice, func(i, j SearchResult) int {
			return (2*(2*j.match-len(search)) - min(j.matching_letters_fn, j.matching_letters_ln)) - (2*(2*i.match-len(search)) - min(i.matching_letters_fn, i.matching_letters_ln))
		})
		topSliceSize := min(10, len(topWordsSlice))
		topWords := []string{}
		for _, w := range topWordsSlice[:topSliceSize] {
			topWords = append(topWords, fmt.Sprintf(`- %s %s %s (%d/%d, %d/%d)`, w.emr_pid, w.first_name, w.last_name, w.matching_letters_fn, len(w.first_name), w.matching_letters_ln, len(w.last_name)))
			// topWords = append(topWords, fmt.Sprintf(`- %s %s -- match: %d, lenMatchWord: %d, diff: %d`, w.first_name, w.last_name, w.match, w.matchWordLen, w.matchWordLen-w.match))
		}

		fmt.Printf(
			"Total matches: %d\nMatches matching whole search: %d\nAverage matching prefix length: %f\nAverage matching word length: %f\nAverage matching word length - matching prefix: %f\nTop %d words:\n%s\n",
			len(values),
			valuesMatchingWholeSearch,
			avgMatch,
			avgMatchWordLen,
			avgMatchWordLenDiff,
			topSliceSize,
			strings.Join(topWords, "\n"),
		)

	case "prep":
		// drop by default
		_, _ = db.Exec("drop table if exists patients_automerge;")
		_, _ = db.Exec("drop table if exists patients_fn_bidx;")
		_, _ = db.Exec("drop table if exists patients_ln_bidx;")
		_, _ = db.Exec("drop table if exists patients;")
		_, _ = db.Exec("drop table if exists customers;")
		// populate db
		_, err = db.Exec(createCustomersTableSQL)
		if err != nil {
			panic(err)
		}
		for i := range ncids {
			_, err = db.Exec("insert into customers (id) values (?);", i+1)
			if err != nil {
				panic(err)
			}
		}

		_, err = db.Exec(createPatientsTableSQL)
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(createFnIndexTableSQL)
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(createLnIndexTableSQL)
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(createAutomergeTableSQL)
		if err != nil {
			panic(err)
		}
	case "populate":
		currentPatient := 1
		chunkSize := 1000
		usersPerCount := 1000000
		affectedRows := 0
		affectedCh := make(chan int64, chunkSize)
		doneWithPopulateCh := make(chan any)
		go func() {
			for affected := range affectedCh {
				affectedRows += int(affected)
				fmt.Printf("done %d/%d...\n", affectedRows, count*usersPerCount)
			}
			doneWithPopulateCh <- nil
		}()
		wg := sync.WaitGroup{}
		parallelConnections := 10
		connlck := make(chan byte, parallelConnections)
		for range parallelConnections {
			connlck <- 0
		}
		parallelConnectionPreps := 10
		preplck := make(chan byte, parallelConnectionPreps)
		for range parallelConnectionPreps {
			preplck <- 0
		}
		for range count {
			wg.Add(usersPerCount / chunkSize)
			randomUsers, err := getRandomUsers(usersPerCount, "de")
			if err != nil {
				panic(err)
			}
			fmt.Printf("got %d random users...\n", len(randomUsers))
			usersChunked := [][]User{}
			for i := range usersPerCount / chunkSize {
				usersChunked = append(usersChunked, randomUsers[i*chunkSize:(i+1)*chunkSize])
			}
			for _, users := range usersChunked {
				<-preplck

				nPatientQueryParams := 6
				nAutomergeQueryParams := 4
				patientsQueryParams := make([]any, chunkSize*nPatientQueryParams)
				fnQueryParams := []any{}
				lnQueryParams := []any{}
				automergeQueryParams := make([]any, chunkSize*nAutomergeQueryParams)

				insertPatientsW := getBuilder()
				insertPatientsW.WriteString(`insert into patients (
				cid,
				emr_pid,
				dob,
				email,
				first_name,
				last_name
				)
				values `)

				insertPatientsFnBidxW := getBuilder()
				insertPatientsFnBidxW.WriteString(`insert into patients_fn_bidx (
				pid,
				cid,
				ibit
				)
				values `)
				insertPatientsLnBidxW := getBuilder()
				insertPatientsLnBidxW.WriteString(`insert into patients_ln_bidx (
				pid,
				cid,
				ibit
				)
				values `)
				insertPatientsAutomergeW := getBuilder()
				insertPatientsAutomergeW.WriteString(`insert into patients_automerge (
				pid,
				cid,
				automerge_full_name_dob,
				automerge_email_dob
				)
				values `)

				for i, user := range users {

					fmt.Fprintf(insertPatientsW, `(?,?,?,?,?,?),`)
					patient_emr_pid := fmt.Sprintf("emr_id_%d", i)
					patient_cid := (i % ncids) + 1 // n customers, spread the data out
					patient_email := fmt.Sprintf("%s.%s@mail.test", user.Name.FirstName, user.Name.LastName)
					patient_dob := time.Now().UTC().Add(-time.Hour * 24 * time.Duration(currentPatient%365)).Format("2006-01-02")
					patientsQueryParams[i*nPatientQueryParams+0] = patient_cid
					patientsQueryParams[i*nPatientQueryParams+1] = patient_emr_pid
					patientsQueryParams[i*nPatientQueryParams+2] = patient_dob
					patientsQueryParams[i*nPatientQueryParams+3] = patient_email
					patientsQueryParams[i*nPatientQueryParams+4] = user.Name.FirstName
					patientsQueryParams[i*nPatientQueryParams+5] = user.Name.LastName

					fmt.Fprintf(insertPatientsAutomergeW, `(?,?,?,?),`)
					hashName := sha256.New().Sum([]byte(user.Name.FirstName + user.Name.LastName + patient_dob))
					hashEmail := sha256.New().Sum([]byte(patient_email + patient_dob))
					automergeQueryParams[i*nAutomergeQueryParams+0] = currentPatient
					automergeQueryParams[i*nAutomergeQueryParams+1] = patient_cid
					automergeQueryParams[i*nAutomergeQueryParams+2] = hashName[:32]
					automergeQueryParams[i*nAutomergeQueryParams+3] = hashEmail[:32]

					lowerFirstNamePadded := strings.ToLower(user.Name.FirstName) + "        "
					firstNameHmacSetBits, _ := getSearchData(lowerFirstNamePadded)
					for _, bit := range firstNameHmacSetBits {
						insertPatientsFnBidxW.WriteString(`(?,?,?),`)
						fnQueryParams = append(fnQueryParams, currentPatient)
						fnQueryParams = append(fnQueryParams, patient_cid)
						fnQueryParams = append(fnQueryParams, bit)
					}

					lowerLastNamePadded := strings.ToLower(user.Name.LastName) + "        "
					lastNameHmacSetBits, _ := getSearchData(lowerLastNamePadded)
					for _, bit := range lastNameHmacSetBits {
						insertPatientsLnBidxW.WriteString(`(?,?,?),`)
						lnQueryParams = append(lnQueryParams, currentPatient)
						lnQueryParams = append(lnQueryParams, patient_cid)
						lnQueryParams = append(lnQueryParams, bit)
					}

					currentPatient++
				}
				patientsQuery := insertPatientsW.String()
				patientsQuery = patientsQuery[0 : len(patientsQuery)-1]
				returnBuilder(insertPatientsW)

				patientsAutomergeQuery := insertPatientsAutomergeW.String()
				patientsAutomergeQuery = patientsAutomergeQuery[0 : len(patientsAutomergeQuery)-1]
				returnBuilder(insertPatientsAutomergeW)

				patientsLnBidxQuery := insertPatientsLnBidxW.String()
				patientsLnBidxQuery = patientsLnBidxQuery[0 : len(patientsLnBidxQuery)-1]
				returnBuilder(insertPatientsLnBidxW)

				patientsFnBidxQuery := insertPatientsFnBidxW.String()
				patientsFnBidxQuery = patientsFnBidxQuery[0 : len(patientsFnBidxQuery)-1]
				returnBuilder(insertPatientsFnBidxW)
				go func() {
					<-connlck
					res, cerr := db.Exec(patientsQuery, patientsQueryParams...)
					if cerr != nil {
						panic(cerr)
					}
					row, _ := res.RowsAffected()
					affectedCh <- row

					_, cerr = db.Exec(patientsAutomergeQuery, automergeQueryParams...)
					if cerr != nil {
						panic(cerr)
					}
					// row, _ = res.RowsAffected()
					// affectedCh <- row

					_, cerr = db.Exec(patientsLnBidxQuery, lnQueryParams...)
					if cerr != nil {
						panic(cerr)
					}
					// row, _ = res.RowsAffected()
					// affectedCh <- row

					_, cerr = db.Exec(patientsFnBidxQuery, fnQueryParams...)
					if cerr != nil {
						panic(cerr)
					}
					// row, _ = res.RowsAffected()
					// affectedCh <- row

					wg.Done()
					connlck <- 0
					preplck <- 0
				}()
			}
			wg.Wait()
		}
		close(affectedCh)
		<-doneWithPopulateCh
		fmt.Printf("affected %d rows\n", affectedRows)
	}
}
