go run match.go config10k.json

go run reindex.go config10k.json

go run raw_to_cols.go config10k.json darkspot
go run raw_to_cols.go config10k.json village

go run reindex_columns.go config10k.json darkspot
go run reindex_columns.go config10k.json village

go run background.go config10k.json

go run subtract.go config10k.json

go run pivot.go config10k.json vis_observed
go run pivot.go config10k.json background
go run pivot.go config10k.json nvalid
go run pivot.go config10k.json bsd
