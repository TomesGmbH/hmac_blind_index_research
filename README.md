Run docker

run `go run ./... -command prep` to set up the tables

run `go run ./... -command populate -count N` to insert `N` copies of the 85000 random names into the reference table `patients_comp` (without blind indexes) and `patients` (with blind indexes)

run `go run ./... -command stats` to see stats (size, number of patients)

run `go run ./... -command search -search <string>` to search for `<string>`. Currently this does not do well with multi words due to my implementation checking the whole string against either the first or last name blind indexes

