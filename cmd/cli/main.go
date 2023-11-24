package main

import (
	"fmt"

	"github.com/anrid/search-bench/pkg/elastic"
	"github.com/anrid/search-bench/pkg/item"
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
	startFrom := pflag.Int("start-from", 0, "start processing items from the Xth item found in data dir")
	benchmarkRuns := pflag.Int("runs", 3, "number of query benchmark runs to execute and average")
	runIndexer := pflag.Bool("run-indexer", false, "recreates bench index, reads items and indexes them in bulk")
	queriesFile := pflag.StringP("queries-file", "q", "", "top queries file (exported from Search logs in BigQuery) [REQUIRED]")
	fetchSource := pflag.Bool("fetch-source", false, "fetch item source when querying items (not just item IDs)")
	createChangeLog := pflag.Bool("create-change-log", false, "create a change log used when running indexing operations during the query benchmark")
	changeLogFile := pflag.String("change-log-file", "", "write change log data to this file")

	pflag.Parse()

	switch *engine {
	case "elastic":
		elastic.SanityTest()

		if *createChangeLog && *dataDir != "" && *changeLogFile != "" {
			item.CreateChangeLog(item.CreateChangeLogArgs{
				ChangeLogFile:  *changeLogFile,
				DataDir:        *dataDir,
				FilenameFilter: *filenameFilter,
				BatchSize:      *batchSize,
				StartFrom:      *startFrom,
				MaxItems:       *max,
			})
		} else if *runIndexer && *dataDir != "" {
			elastic.RunIndexer(elastic.RunIndexerArgs{
				DataDir:        *dataDir,
				FilenameFilter: *filenameFilter,
				BatchSize:      *batchSize,
				Max:            *max,
			})
		} else if *queriesFile != "" {
			queries := query.Load(*queriesFile)

			elastic.RunBenchmark(*benchmarkRuns, queries, *fetchSource)
		} else {
			fmt.Println("Not enough flags given")
			pflag.PrintDefaults()
		}
	case "manticore":
		fmt.Printf("TODO!")
	default:
		fmt.Printf("Unsupported search engine '%s'\n", *engine)
		pflag.PrintDefaults()
	}
}
