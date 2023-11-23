package main

import (
	"fmt"
	"os"

	"github.com/anrid/search-bench/pkg/elastic"
	"github.com/anrid/search-bench/pkg/query"
	"github.com/spf13/pflag"
)

const (
	ESHost                = "http://127.0.0.1:9200"
	ESSanityTestIndexName = "test"
	ESBenchIndexName      = "items"
	DebugPrint            = false
)

type Map = map[string]interface{}

func main() {
	engine := pflag.StringP("engine", "e", "elastic", "search engine to use [elastic | manticore] (default: elastic) [REQUIRED]")
	dataDir := pflag.StringP("data-dir", "d", "", "data dir containing Item files in CSV format (gzipped) [REQUIRED]")
	filenameFilter := pflag.StringP("filename-filter", "f", ".csv.gz", "filename pattern to filter on in data dir")
	batchSize := pflag.Int("batch-size", 5000, "batch size, i.e. number of items to insert into ES at a time")
	max := pflag.Int("max", 0, "process max X items before exiting")
	runIndexer := pflag.Bool("run-indexer", false, "recreates bench index, reads items and indexes them in bulk")
	queriesFile := pflag.StringP("queries-file", "q", "", "top queries file (exported from Search logs in BigQuery) [REQUIRED]")
	fetchSource := pflag.Bool("fetch-source", false, "fetch item source when querying items (not just item IDs)")

	pflag.Parse()

	if *dataDir == "" || *queriesFile == "" {
		pflag.PrintDefaults()
		os.Exit(-1)
	}

	queries := query.Load(*queriesFile)

	switch *engine {
	case "elastic":
		elastic.SanityTest()
		if *runIndexer {
			elastic.RunIndexer(elastic.RunIndexerArgs{
				DataDir:        *dataDir,
				FilenameFilter: *filenameFilter,
				BatchSize:      *batchSize,
				Max:            *max,
			})
		}

		elastic.RunBenchmark(3, queries, *fetchSource)
	case "manticore":
		fmt.Printf("TODO!")
	default:
		fmt.Printf("Unsupported search engine '%s'\n", *engine)
		pflag.PrintDefaults()
	}
}
