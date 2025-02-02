package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	_ "embed"

	_ "github.com/go-sql-driver/mysql"
)

var key = []byte("qhmQvFgKBJGaaRADCUmLMMb0lVtm6fLq")

func createHmac(plain string) byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(plain))
	return mac.Sum(nil)[0]
}

func combineHmacs(letters1 byte, letters2 byte, letters3 byte, letters4 byte, letters5 byte, letters6 byte) uint32 {
	return uint32(
		((uint32(letters1)&0x1f)<<0)|
			((uint32(letters2)&0x0f)<<5)|
			((uint32(letters3)&0x0f)<<9)|
			((uint32(letters4)&0x0f)<<13)|
			((uint32(letters5)&0x0f)<<17)|
			((uint32(letters6)&0x07)<<21),
	) & 0xffffff
	// ((int32(letters5) & 0x3f) << 24))
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

var lastRequest = time.Time{}

var lck = sync.Mutex{}

const throttleRandomUsersSeconds = 20

func getRandomUsers(amount int, nationality string) ([]User, error) {
	lck.Lock()
	defer lck.Unlock()
	sinceLast := time.Since(lastRequest)
	fmt.Printf("last requested users %d ago at %s\n", sinceLast, lastRequest.Format("3:04:05PM"))
	if sinceLast <= (throttleRandomUsersSeconds * time.Second) {
		fmt.Printf("Waiting %d seconds to make api request to respect their limits\n", (throttleRandomUsersSeconds*time.Second-sinceLast)/time.Second)
		<-time.After(throttleRandomUsersSeconds*time.Second - sinceLast)
	}
	for {
		lastRequest = time.Now()
		res, err := http.Get(fmt.Sprintf("https://randomuser.me/api?results=%d&nat=%s&inc=name&noinfo", amount, nationality))
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		decoder := json.NewDecoder(res.Body)
		usersResponse := GetUsersResponse{Results: []User{}}
		err = decoder.Decode(&usersResponse)
		if err != nil {
			return nil, err
		}
		if usersResponse.Error == "" {
			return usersResponse.Results, nil
		}
		fmt.Printf("Error while getting random users: %q. Retrying...\n", usersResponse.Error)
		<-time.After(120 * time.Second)
	}
}

func queryFor(db *sql.DB, search string) (*sql.Rows, error) {
	switch len(search) {
	case 1:
		hmac := combineHmacs(createHmac(search[:1]), 0, 0, 0, 0, 0)
		return db.Query(`select first_name, last_name from patients where 
			first_name_bidx & X'1F' = ? 
			or last_name_bidx & X'1F' = ?
		--	LIMIT 1000
			;`,
			hmac,
			hmac,
		)
	case 2:
		hmac := combineHmacs(createHmac(search[:1]), createHmac(search[:2]), 0, 0, 0, 0)
		return db.Query(`select first_name, last_name from patients where 
			first_name_bidx & X'01FF' = ? 
			or last_name_bidx & X'01FF' = ?
		--	LIMIT 1000;`,
			hmac,
			hmac,
		)
	case 3:
		hmac := combineHmacs(createHmac(search[:1]), createHmac(search[:2]), createHmac(search[:3]), 0, 0, 0)
		return db.Query(`select first_name, last_name from patients where 
			first_name_bidx & X'1FFF' = ?
			or last_name_bidx & X'1FFF' = ?
			-- LIMIT 1000;`,
			hmac,
			hmac,
		)
	case 4:
		hmac := combineHmacs(createHmac(search[:1]), createHmac(search[:2]), createHmac(search[:3]), createHmac(search[:4]), 0, 0)
		return db.Query(`select first_name, last_name from patients where 
			first_name_bidx & X'01FFFF' = ? 
			or last_name_bidx & X'01FFFF' = ?
			-- LIMIT 1000;`,
			hmac,
			hmac,
		)
	case 5:
		hmac := combineHmacs(createHmac(search[:1]), createHmac(search[:2]), createHmac(search[:3]), createHmac(search[:4]), createHmac(search[:5]), 0)
		return db.Query(`select first_name, last_name from patients where 
			first_name_bidx & X'1FFFFF' = ? 
			or last_name_bidx & X'1FFFFF' = ?
			-- LIMIT 1000;`,
			hmac,
			hmac,
		)
	default:
		hmac := combineHmacs(createHmac(search[:1]), createHmac(search[:2]), createHmac(search[:3]), createHmac(search[:4]), createHmac(search[:5]), createHmac(search[:6]))
		return db.Query(`select first_name, last_name from patients where 
			first_name_bidx & X'FFFFFF' = ? 
			or last_name_bidx & X'FFFFFF' = ?
			-- LIMIT 1000;`,
			hmac,
			hmac,
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

func main() {
	var command string
	flag.StringVar(&command, "command", "search", "enter 'search', 'populate', 'stats', or 'prep'")
	var search string
	flag.StringVar(&search, "search", "", "enter text to search if searching")
	var count int
	flag.IntVar(&count, "count", 1, "enter loops of inserting")
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
		patientCompIndexSizes := []int{}
		patientCompIndexNames := []string{}
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
			case "patients_comp":
				patientCompIndexNames = append(patientCompIndexNames, index)
				patientCompIndexSizes = append(patientCompIndexSizes, indexSizeBytes)
			}
		}
		fmt.Printf("Table: patients\n\t")
		totalSize := 0
		for i, sizeBytes := range patientIndexSizes {
			fmt.Printf("- Index: %s\n\t  Size: %.2f\n\t", patientIndexNames[i], float64(sizeBytes)/1024.0/1024.0)
			totalSize += sizeBytes
		}
		fmt.Printf("\bTotal Size: %.2f\n\n", float64(totalSize)/1024.0/1024.0)
		fmt.Printf("Table: patients_comp\n\t")
		totalSize = 0
		for i, sizeBytes := range patientCompIndexSizes {
			fmt.Printf("- Index: %s\n\t  Size: %.2f\n\t", patientCompIndexNames[i], float64(sizeBytes)/1024.0/1024.0)
			totalSize += sizeBytes
		}
		fmt.Printf("\bTotal Size: %.2f\n\n", float64(totalSize)/1024.0/1024.0)

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
		fmt.Printf("hmac\n")
		rows, err := queryFor(db, search)
		if err != nil {
			panic(err)
		}
		values := []SearchResult{}
		for rows.Next() {
			value := SearchResult{}
			err = rows.Scan(&value.first_name, &value.last_name)
			if err != nil {
				panic(err)
			}
			value.matching_letters_fn = 0
			value.matching_letters_ln = 0
			for i, l := range []byte(search) {
				if len(value.first_name) > i {
					if value.first_name[i] == l {
						value.matching_letters_fn++
					}
				}
				if len(value.last_name) > i {
					if value.last_name[i] == l {
						value.matching_letters_ln++
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
			return (i.matchWordLen - i.match) - (j.matchWordLen - j.match)
		})
		topSliceSize := min(10, len(topWordsSlice))
		topWords := []string{}
		for _, w := range topWordsSlice[:topSliceSize] {
			topWords = append(topWords, fmt.Sprintf(`- %s %s`, w.first_name, w.last_name))
			// topWords = append(topWords, fmt.Sprintf(`- %s %s -- match: %d, lenMatchWord: %d, diff: %d`, w.first_name, w.last_name, w.match, w.matchWordLen, w.matchWordLen-w.match))
		}

		fmt.Printf(
			"Total matches: %d\naverage matching prefix length: %f\naverage matching word length: %f\naverage matching word length - matching prefix: %f\nTop %d words:\n%s\n",
			len(values),
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
		chunkSize := 100
		usersPerCount := 5000
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

					firstNamePadded := user.Name.FirstName + "        "
					lastNamePadded := user.Name.LastName + "        "
					firstNameHmac1 := createHmac(firstNamePadded[:1])
					firstNameHmac2 := createHmac(firstNamePadded[:2])
					firstNameHmac3 := createHmac(firstNamePadded[:3])
					firstNameHmac4 := createHmac(firstNamePadded[:4])
					firstNameHmac5 := createHmac(firstNamePadded[:5])
					firstNameHmac6 := createHmac(firstNamePadded[:6])
					lastNameHmac1 := createHmac(lastNamePadded[:1])
					lastNameHmac2 := createHmac(lastNamePadded[:2])
					lastNameHmac3 := createHmac(lastNamePadded[:3])
					lastNameHmac4 := createHmac(lastNamePadded[:4])
					lastNameHmac5 := createHmac(lastNamePadded[:5])
					lastNameHmac6 := createHmac(lastNamePadded[:6])
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
