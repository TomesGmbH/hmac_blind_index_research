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

	_ "embed"

	_ "github.com/go-sql-driver/mysql"
)

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

func bitMask(letterCount int) uint32 {
	bitMask := uint32(0)
	for i := range truncLetterCount(letterCount) {
		bitMask = bitMask | bitPattern(i+1)<<bitShift(i+1)
	}
	return bitMask
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

type SearchResult struct {
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

// var lastRequest = time.Time{}

var lck = sync.Mutex{}

// const throttleRandomUsersSeconds = 20

func getRandomUsers(amount int, _ string) ([]User, error) {
	lck.Lock()
	defer lck.Unlock()
	reader := csv.NewReader(namesReader)
	reader.FieldsPerRecord = 2
	users := make([]User, amount)
	for i := range amount {
		// offset := reader.InputOffset()
		// fmt.Println(offset)
		record, err := reader.Read()
		if err != nil {
			return nil, err
		}
		users[i].Name = UserName{FirstName: record[0], LastName: record[1]}
	}
	return users, nil
	// lck.Lock()
	// defer lck.Unlock()
	// sinceLast := time.Since(lastRequest)
	// fmt.Printf("last requested users %d ago at %s\n", sinceLast, lastRequest.Format("3:04:05PM"))
	// if sinceLast <= (throttleRandomUsersSeconds * time.Second) {
	// 	fmt.Printf("Waiting %d seconds to make api request to respect their limits\n", (throttleRandomUsersSeconds*time.Second-sinceLast)/time.Second)
	// 	<-time.After(throttleRandomUsersSeconds*time.Second - sinceLast)
	// }
	// for {
	// 	lastRequest = time.Now()
	// 	res, err := http.Get(fmt.Sprintf("https://randomuser.me/api?results=%d&nat=%s&inc=name&noinfo", amount, nationality))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	defer res.Body.Close()
	// 	decoder := json.NewDecoder(res.Body)
	// 	usersResponse := GetUsersResponse{Results: []User{}}
	// 	err = decoder.Decode(&usersResponse)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if usersResponse.Error == "" {
	// 		return usersResponse.Results, nil
	// 	}
	// 	fmt.Printf("Error while getting random users: %q. Retrying...\n", usersResponse.Error)
	// 	<-time.After(120 * time.Second)
	// }
}

func queryFor(db *sql.DB, search string) (*sql.Rows, error) {
	switch len(search) {
	case 1:
		hmac := combineHmacs(createHmac(search[:1], key1), 0, 0, 0, 0, 0)
		fmt.Printf("hmac: 0x%x\n", hmac)
		return db.Query(`select first_name, last_name from patients where 
			(first_name_bidx & ?) = ? 
			or (last_name_bidx & ?) = ?
			LIMIT ?
			;`,
			bitMask(1),
			hmac,
			bitMask(1),
			hmac,
			searchLimit,
		)
	case 2:
		hmac := combineHmacs(createHmac(search[:1], key1), createHmac(search[:2], key2), 0, 0, 0, 0)
		fmt.Printf("hmac: 0x%x\n", hmac)
		return db.Query(`select first_name, last_name from patients where 
			(first_name_bidx & ?) = ? 
			or (last_name_bidx & ?) = ?
	    	LIMIT ?;`,
			bitMask(2),
			hmac,
			bitMask(2),
			hmac,
			searchLimit,
		)
	case 3:
		hmac := combineHmacs(createHmac(search[:1], key1), createHmac(search[:2], key2), createHmac(search[:3], key3), 0, 0, 0)
		fmt.Printf("hmac: 0x%x\n", hmac)
		return db.Query(`select first_name, last_name from patients where 
			(first_name_bidx & ?) = ?
			or (last_name_bidx & ?) = ?
			LIMIT ?;`,
			bitMask(3),
			hmac,
			bitMask(3),
			hmac,
			searchLimit,
		)
	case 4:
		hmac := combineHmacs(createHmac(search[:1], key1), createHmac(search[:2], key2), createHmac(search[:3], key3), createHmac(search[:4], key4), 0, 0)
		fmt.Printf("hmac: 0x%x\n", hmac)
		return db.Query(`select first_name, last_name from patients where 
			(first_name_bidx & ?) = ? 
			or (last_name_bidx & ?) = ?
			LIMIT ?;`,
			bitMask(4),
			hmac,
			bitMask(4),
			hmac,
			searchLimit,
		)
	case 5:
		hmac := combineHmacs(createHmac(search[:1], key1), createHmac(search[:2], key2), createHmac(search[:3], key3), createHmac(search[:4], key4), createHmac(search[:5], key5), 0)
		fmt.Printf("hmac: 0x%x\n", hmac)
		return db.Query(`select first_name, last_name from patients where 
			(first_name_bidx & ?) = ? 
			or (last_name_bidx & ?) = ?
			LIMIT ?;`,
			bitMask(5),
			hmac,
			bitMask(5),
			hmac,
			searchLimit,
		)
	default:
		hmac := combineHmacs(createHmac(search[:1], key1), createHmac(search[:2], key2), createHmac(search[:3], key3), createHmac(search[:4], key4), createHmac(search[:5], key5), createHmac(search[:6], key6))
		fmt.Printf("hmac: 0x%x\n", hmac)
		return db.Query(`select first_name, last_name from patients where 
			(first_name_bidx & ?) = ? 
			or (last_name_bidx & ?) = ?
			LIMIT ?;`,
			bitMask(6),
			hmac,
			bitMask(6),
			hmac,
			searchLimit,
		)
		// default:
		// 	hmac := combineHmacs(createHmac(search[:1]), createHmac(search[:2]), createHmac(search[:3]), createHmac(search[:4]), createHmac(search[:5]))
		// 	return db.Query(`select first_name, last_name from patients where
		// 		first_name_bidx & 0x3fffffff = ?
		// 		or last_name_bidx & 0x3fffffff = ?
		// 		LIMIT 1000;`,
		// 		hmac,
		// 		hmac,
		// 	)
		// default:
		// 	searchPadded := search + "       "
		// 	hmac1 := createHmac(search[:1])
		// 	hmac2 := createHmac(searchPadded[:2])
		// 	hmac3 := createHmac(searchPadded[:3])
		// 	hmac4 := createHmac(searchPadded[:4])
		// 	hmac5 := createHmac(searchPadded[:5])
		// 	return db.Query(`select first_name, last_name from patients where
		// 		(first_name_bidx1 = ? and first_name_bidx2 = ? and first_name_bidx3 = ? and first_name_bidx4 = ? and first_name_bidx5 = ?)
		// 		or (last_name_bidx1 = ? and last_name_bidx2 = ? and last_name_bidx3 = ? and last_name_bidx4 = ? and last_name_bidx5 = ?)
		// 		;`,
		// 		hmac1, hmac2, hmac3, hmac4, hmac5,
		// 		hmac1, hmac2, hmac3, hmac4, hmac5,
		// 	)
	}
}

var searchLimit int

func main() {
	var command string
	flag.StringVar(&command, "command", "search", "enter 'search', 'populate', 'stats', or 'prep'")
	var search string
	flag.StringVar(&search, "search", "", "enter text to search if searching")
	var count int
	flag.IntVar(&count, "count", 1, "enter loops of inserting")
	flag.IntVar(&searchLimit, "limit", 1000, "enter search limit")
	flag.Parse()

	db, err := sql.Open("mysql", "root:password@tcp(127.0.0.1:3306)/db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	fmt.Println(command)
	switch command {
	case "stats":
		querySize := `SELECT table_name, index_name,
stat_value * @@innodb_page_size as size_in_mb
FROM mysql.innodb_index_stats
WHERE stat_name = 'size'
AND table_name = 'patients' 
OR table_name = 'patients_comp'
;`
		res, err := db.Query(querySize)
		if err != nil {
			panic(err)
		}
		patientIndexSizes := []int{}
		patientIndexNames := []string{}
		// patientCompIndexSizes := []int{}
		// patientCompIndexNames := []string{}
		for res.Next() {
			var tbl string
			var index string
			var indexSizeBytes int
			err = res.Scan(&tbl, &index, &indexSizeBytes)
			if err != nil {
				panic(err)
			}
			switch tbl {
			case "patients":
				patientIndexNames = append(patientIndexNames, index)
				patientIndexSizes = append(patientIndexSizes, indexSizeBytes)
				// case "patients_comp":
				// 	patientCompIndexNames = append(patientCompIndexNames, index)
				// 	patientCompIndexSizes = append(patientCompIndexSizes, indexSizeBytes)
			}
		}
		fmt.Printf("Table: patients\n\t")
		totalSize := 0
		for i, sizeBytes := range patientIndexSizes {
			if patientIndexNames[i] == "PRIMARY" {
				fmt.Printf("- Table Data:\n\t  Size: %.2f MB\n\t", float64(sizeBytes)/1024.0/1024.0)
			} else {
				fmt.Printf("- Index: %s\n\t  Size: %.2f MB\n\t", patientIndexNames[i], float64(sizeBytes)/1024.0/1024.0)
			}
			totalSize += sizeBytes
		}
		fmt.Printf("\bTotal Size: %.2f MB\n\n", float64(totalSize)/1024.0/1024.0)
		// fmt.Printf("Table: patients_comp\n\t")
		// totalSize = 0
		// for i, sizeBytes := range patientCompIndexSizes {
		// 	fmt.Printf("- Index: %s\n\t  Size: %.2f\n\t", patientCompIndexNames[i], float64(sizeBytes)/1024.0/1024.0)
		// 	totalSize += sizeBytes
		// }
		// fmt.Printf("\bTotal Size: %.2f\n\n", float64(totalSize)/1024.0/1024.0)

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
		rows, err := queryFor(db, search)
		if err != nil {
			panic(err)
		}
		values := []SearchResult{}
		valuesMatchingWholeSearch := 0
		for rows.Next() {
			value := SearchResult{}
			err = rows.Scan(&value.first_name, &value.last_name)
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
			topWords = append(topWords, fmt.Sprintf(`- %s %s (%d/%d, %d/%d)`, w.first_name, w.last_name, w.matching_letters_fn, len(w.first_name), w.matching_letters_ln, len(w.last_name)))
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
		_, _ = db.Exec("drop table patients;")
		_, _ = db.Exec("drop table patients_comp;")
		// populate db
		_, err = db.Exec(`create table patients (
						  id SERIAL PRIMARY KEY,
						  first_name VARCHAR(64) NOT NULL,
						  first_name_bidx MEDIUMINT UNSIGNED NOT NULL,
						  last_name VARCHAR(64) NOT NULL,
						  last_name_bidx MEDIUMINT UNSIGNED NOT NULL,
						  INDEX (first_name_bidx),
						  INDEX (last_name_bidx)
						);`,
		)
		if err != nil {
			panic(err)
		}
		// _, err = db.Exec(`create table patients_comp (
		// 				  id SERIAL PRIMARY KEY,
		// 				  first_name_cmp VARCHAR(100) NOT NULL,
		// 				  last_name_cmp VARCHAR(100) NOT NULL
		// 				);`,
		// )
		// if err != nil {
		// 	panic(err)
		// }
	case "populate":
		chunkSize := 10000
		usersPerCount := 1000000
		affectedRows := 0
		affectedCh := make(chan int64, 1000)
		doneWithPopulateCh := make(chan any)
		go func() {
			for affected := range affectedCh {
				affectedRows += int(affected)
				fmt.Printf("done %d/%d...\n", affectedRows, count*usersPerCount)
			}
			doneWithPopulateCh <- nil
		}()
		wg := sync.WaitGroup{}
		lck := make(chan byte, 20)
		for range 20 {
			lck <- 0
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

				nQueryParams := 4
				queryParams := make([]interface{}, chunkSize*nQueryParams)
				// nCompQueryParams := 2
				// compQueryParams := make([]interface{}, chunkSize*nCompQueryParams)
				// patientsCompQuery := `insert into patients_comp (
				// first_name,
				// last_name)
				// values `
				patientsQuery := `insert into patients (
				first_name,
				first_name_bidx,
				last_name,
				last_name_bidx
				)
				values `

				for i, user := range users {
					// patientsCompQuery += `(?,?),`
					// compQueryParams[i*nCompQueryParams+0] = user.Name.FirstName
					// compQueryParams[i*nCompQueryParams+1] = user.Name.LastName

					lowerFirstNamePadded := strings.ToLower(user.Name.FirstName) + "        "
					lowerLastNamePadded := strings.ToLower(user.Name.LastName) + "        "
					firstNameHmac1 := createHmac(lowerFirstNamePadded[:1], key1)
					firstNameHmac2 := createHmac(lowerFirstNamePadded[:2], key2)
					firstNameHmac3 := createHmac(lowerFirstNamePadded[:3], key3)
					firstNameHmac4 := createHmac(lowerFirstNamePadded[:4], key4)
					firstNameHmac5 := createHmac(lowerFirstNamePadded[:5], key5)
					firstNameHmac6 := createHmac(lowerFirstNamePadded[:6], key6)
					lastNameHmac1 := createHmac(lowerLastNamePadded[:1], key1)
					lastNameHmac2 := createHmac(lowerLastNamePadded[:2], key2)
					lastNameHmac3 := createHmac(lowerLastNamePadded[:3], key3)
					lastNameHmac4 := createHmac(lowerLastNamePadded[:4], key4)
					lastNameHmac5 := createHmac(lowerLastNamePadded[:5], key5)
					lastNameHmac6 := createHmac(lowerLastNamePadded[:6], key6)
					patientsQuery += `(?,?,?,?),`
					queryParams[i*nQueryParams+0] = user.Name.FirstName
					queryParams[i*nQueryParams+1] = combineHmacs(firstNameHmac1, firstNameHmac2, firstNameHmac3, firstNameHmac4, firstNameHmac5, firstNameHmac6)
					// queryParams[i*nQueryParams+2] = firstNameHmac2
					// queryParams[i*nQueryParams+3] = firstNameHmac3
					// queryParams[i*nQueryParams+4] = firstNameHmac4
					queryParams[i*nQueryParams+2] = user.Name.LastName
					queryParams[i*nQueryParams+3] = combineHmacs(lastNameHmac1, lastNameHmac2, lastNameHmac3, lastNameHmac4, lastNameHmac5, lastNameHmac6)
					// queryParams[i*nQueryParams+7] = lastNameHmac2
					// queryParams[i*nQueryParams+8] = lastNameHmac3
					// queryParams[i*nQueryParams+9] = lastNameHmac4
				}
				// patientsCompQuery = patientsCompQuery[0 : len(patientsCompQuery)-1]
				patientsQuery = patientsQuery[0 : len(patientsQuery)-1]

				go func() {
					<-lck
					// _, cerr := db.Exec(patientsCompQuery, compQueryParams...)
					// if cerr != nil {
					// 	panic(cerr)
					// }
					res, cerr := db.Exec(patientsQuery, queryParams...)
					if cerr != nil {
						panic(cerr)
					}

					row, _ := res.RowsAffected()
					affectedCh <- row
					wg.Done()
					lck <- 0
				}()
			}
			wg.Wait()
		}
		close(affectedCh)
		<-doneWithPopulateCh
		fmt.Printf("affected %d rows\n", affectedRows)
	}
}
