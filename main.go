package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
	"sync"

	_ "embed"

	_ "github.com/go-sql-driver/mysql"
)

//go:embed random_names.csv
var nameList string

//go:embed insert_random_names.sql
var insertNamesCmp string

var key = []byte("qhmQvFgKBJGaaRADCUmLMMb0lVtm6fLq")

func createHmac(plain string) byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(plain))
	return mac.Sum(nil)[0]
}

type SearchResult struct {
	first_name, last_name string
	matching_letters_fn   int
	matching_letters_ln   int
	match                 int
	matchWordLen          int
}

func queryFor(db *sql.DB, search string) (*sql.Rows, error) {
	switch len(search) {
	case 1:
		hmac1 := createHmac(search[:1])
		return db.Query(`select first_name, last_name from patients where 
			first_name_bidx1 = ? 
			or last_name_bidx1 = ?
			LIMIT 1000;`,
			hmac1,
			hmac1,
		)
	case 2:
		hmac1 := createHmac(search[:1])
		hmac2 := createHmac(search[:2])
		return db.Query(`select first_name, last_name from patients where 
			(first_name_bidx1 = ? and first_name_bidx2 = ?) 
			or (last_name_bidx1 = ? and last_name_bidx2 = ?)
			LIMIT 1000;`,
			hmac1, hmac2,
			hmac1, hmac2,
		)
	case 3:
		searchPadded := search
		hmac1 := createHmac(search[:1])
		hmac2 := createHmac(searchPadded[:2])
		hmac3 := createHmac(searchPadded[:3])
		return db.Query(`select first_name, last_name from patients where 
			(first_name_bidx1 = ? and first_name_bidx2 = ? and first_name_bidx3 = ?)  
			or (last_name_bidx1 = ? and last_name_bidx2 = ? and last_name_bidx3 = ?)
			LIMIT 1000;`,
			hmac1, hmac2, hmac3,
			hmac1, hmac2, hmac3,
		)
	default:
		searchPadded := search
		hmac1 := createHmac(search[:1])
		hmac2 := createHmac(searchPadded[:2])
		hmac3 := createHmac(searchPadded[:3])
		hmac4 := createHmac(searchPadded[:4])
		return db.Query(`select first_name, last_name from patients where 
			(first_name_bidx1 = ? and  first_name_bidx2 = ? and first_name_bidx3 = ? and first_name_bidx4 = ?) 
			or (last_name_bidx1 = ? and last_name_bidx2 = ? and last_name_bidx3 = ? and last_name_bidx4 = ?)
			LIMIT 1000;`,
			hmac1, hmac2, hmac3, hmac4,
			hmac1, hmac2, hmac3, hmac4,
		)
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
						  first_name TEXT NOT NULL,
						  first_name_bidx1 tinyint unsigned NOT NULL, -- 1 letter
						  first_name_bidx2 tinyint unsigned NOT NULL, -- 2 letters
						  first_name_bidx3 tinyint unsigned NOT NULL, -- 3 letters
						  first_name_bidx4 tinyint unsigned NOT NULL, -- 4 letters
						  last_name TEXT NOT NULL,
						  last_name_bidx1 tinyint unsigned NOT NULL, -- 1 letter
						  last_name_bidx2 tinyint unsigned NOT NULL, -- 2 letters
						  last_name_bidx3 tinyint unsigned NOT NULL, -- 3 letters
						  last_name_bidx4 tinyint unsigned NOT NULL, -- 4 letters
						  INDEX (first_name_bidx1),
						  INDEX (first_name_bidx1, first_name_bidx2),
						  INDEX (first_name_bidx1, first_name_bidx2, first_name_bidx3),
						  INDEX (first_name_bidx1, first_name_bidx2, first_name_bidx3, first_name_bidx4),
						  INDEX (last_name_bidx1),
						  INDEX (last_name_bidx1, last_name_bidx2),
						  INDEX (last_name_bidx1, last_name_bidx2, last_name_bidx3),
						  INDEX (last_name_bidx1, last_name_bidx2, last_name_bidx3, last_name_bidx4)
						);`,
		)
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(`create table patients_comp (
						  id SERIAL PRIMARY KEY,
						  first_name TEXT NOT NULL,
						  last_name TEXT NOT NULL
						);`,
		)
		if err != nil {
			panic(err)
		}
	case "populate":
		affectedRows := 0
		for range count {
			_, err = db.Exec(insertNamesCmp)
			if err != nil {
				panic(err)
			}

			res, err := db.Query("SELECT TABLE_NAME AS `Table`, ROUND((DATA_LENGTH + INDEX_LENGTH)) AS `Size (B)` FROM information_schema.TABLES WHERE TABLE_NAME='patients' or TABLE_NAME='patients_comp'")
			if err != nil {
				panic(err)
			}
			for res.Next() {
				var tbl string
				var sizeMb int
				err = res.Scan(&tbl, &sizeMb)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Table: %s, Size (B): %d\n", tbl, sizeMb)
			}
			r := csv.NewReader(strings.NewReader(nameList))

			affectedCh := make(chan int64, 1000)

			go func() {
				for affected := range affectedCh {
					affectedRows += int(affected)
					if affectedRows%1000 == 0 {
						fmt.Printf("done %d/%d...\n", affectedRows, count*85000)
					}
				}
			}()
			wg := sync.WaitGroup{}
			lck := make(chan byte, 20)
			for range 20 {
				lck <- 0
			}
			for {
				record, cerr := r.Read()
				if cerr == io.EOF {
					break
				}
				if cerr != nil {
					log.Fatal(cerr)
				}

				// fmt.Printf("name splitting\n")
				firstName := record[0]
				lastName := record[1]

				<-lck
				wg.Add(1)
				go func() {
					// padding to guarantee length
					firstNamePadded := firstName + "      "
					lastNamePadded := lastName + "      "
					firstNameHmac1 := createHmac(firstNamePadded[:1])
					firstNameHmac2 := createHmac(firstNamePadded[:2])
					firstNameHmac3 := createHmac(firstNamePadded[:3])
					firstNameHmac4 := createHmac(firstNamePadded[:4])
					// firstNameHmac5 := createHmac(firstNamePadded[:5])
					lastNameHmac1 := createHmac(lastNamePadded[:1])
					lastNameHmac2 := createHmac(lastNamePadded[:2])
					lastNameHmac3 := createHmac(lastNamePadded[:3])
					lastNameHmac4 := createHmac(lastNamePadded[:4])
					// lastNameHmac5 := createHmac(lastNamePadded[:5])

					res, cerr := db.Exec(`insert into patients (
				first_name, 
				first_name_bidx1, 
				first_name_bidx2, 
				first_name_bidx3, 
				first_name_bidx4,
		--		first_name_bidx5, 
				last_name, 
				last_name_bidx1, 
				last_name_bidx2, 
				last_name_bidx3, 
				last_name_bidx4 
		--		last_name_bidx5
				)
				values (
				?,
				?,
				?,
				?,
				?,
			--	?,
			--	?,
				?,
				?,
				?,
				?,
				?)`,
						firstName,
						firstNameHmac1,
						firstNameHmac2,
						firstNameHmac3,
						firstNameHmac4,
						// firstNameHmac5,
						lastName,
						lastNameHmac1,
						lastNameHmac2,
						lastNameHmac3,
						lastNameHmac4,
					// lastNameHmac5
					)
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
			close(affectedCh)
			fmt.Printf("affected %d rows\n", affectedRows)
			res, err = db.Query("SELECT TABLE_NAME AS `Table`, ROUND((DATA_LENGTH + INDEX_LENGTH)) AS `Size (B)` FROM information_schema.TABLES WHERE TABLE_NAME='patients' or TABLE_NAME='patients_comp'")
			if err != nil {
				panic(err)
			}
			for res.Next() {
				var tbl string
				var sizeMb int
				err = res.Scan(&tbl, &sizeMb)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Table: %s, Size (B): %d\n", tbl, sizeMb)
			}
		}
	}
}
