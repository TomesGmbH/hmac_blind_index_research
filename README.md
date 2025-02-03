Run docker

run `go run ./... -command prep` to set up the tables

run `go run ./... -command populate -count N` to insert `N` copies of the 1,000,000 random names into `patients` (with blind indexes). I recommend `count=1`

run `go run ./... -command stats` to see stats (size, number of patients)

run `go run ./... -command search -limit N -search <string>` to search for `<string>`. Currently this does not do well with multi word first names due to my implementation checking the whole search string against either the first or last name blind indexes only

