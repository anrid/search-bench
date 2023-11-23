package main

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/ikawaha/kagome-dict/ipa"
	"github.com/ikawaha/kagome/v2/tokenizer"
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

	queries := LoadQueries(*queriesFile)

	ESSanityTest()

	if *runIndexer {
		PrepareBenchIndex()

		start := time.Now()

		ReadItems(*dataDir, *filenameFilter, *batchSize, *max, ProcessItems)

		ESRefreshIndex(ESBenchIndexName)
		stats := ESGetIndexStats(ESBenchIndexName)

		fmt.Printf("Finished indexing %d items in %s\n", stats.All.Primaries.Docs.Count, time.Since(start))
	}

	stats := ESGetIndexStats(ESBenchIndexName)
	fmt.Printf("Index stats:\n%s\n", ToPrettyJSON(stats))

	start := time.Now()

	QueryItems(queries, *fetchSource, 240)

	fmt.Printf("Executed %d queries in %s\n", len(queries), time.Since(start))
}

type SearchQuery struct {
	Keyword     string
	CategoryIDs []int64
	Statuses    []Status
}

type RawSearchQuery struct {
	Query string `json:"query"`
	Count string `json:"c"`
}

func LoadQueries(queriesFile string) (qs []*SearchQuery) {
	fmt.Printf("Loading queries from %s ..\n", queriesFile)

	b, err := os.ReadFile(queriesFile)
	if err != nil {
		log.Panic(err)
	}

	qs = make([]*SearchQuery, 0)
	raws := make([]*RawSearchQuery, 0)

	err = sonic.Unmarshal(b, &raws)
	if err != nil {
		log.Panic(err)
	}

	tok := getTokenizer()

	for _, r := range raws {
		q := new(SearchQuery)
		parts := strings.SplitN(r.Query, "<|>", 3)
		if len(parts) != 3 {
			log.Panicf("expected 3 parts in raw query: '%s'", r.Query)
		}

		if parts[0] != "" {
			// Handle keywords
			keywordParts := tok.Wakati(parts[0])
			q.Keyword = strings.Join(keywordParts, " ")
		}

		if len(parts[1]) > 2 {
			// Handle category IDs array in JSON string format
			trimmed := parts[1][1 : len(parts[1])-1]
			cats := strings.SplitN(trimmed, ",", -1)
			for _, cat := range cats {
				q.CategoryIDs = append(q.CategoryIDs, ToInt(cat))
			}
		}

		if len(parts[2]) > 2 {
			// Handle statuses array in JSON string format
			trimmed := parts[2][1 : len(parts[2])-1]
			statuses := strings.SplitN(trimmed, ",", -1)
			for _, status := range statuses {
				switch status {
				case "ITEM_STATUS_ON_SALE":
					q.Statuses = append(q.Statuses, StatusOnSale)
				case "ITEM_STATUS_TRADING":
					q.Statuses = append(q.Statuses, StatusTrading)
				case "ITEM_STATUS_SOLD_OUT":
					q.Statuses = append(q.Statuses, StatusSold)
				}
			}
		}

		qs = append(qs, q)
	}

	fmt.Printf("Loaded and prepared %d queries\n", len(qs))

	return
}

func QueryItems(queries []*SearchQuery, fetchSource bool, fetchMax int) {
	size := 120
	var qc int

	for _, q := range queries {
		qc++

		var from int64
		var totalDocsFetched int

		boolQuery := Map{}

		if q.Keyword != "" {
			boolQuery["should"] = []Map{
				{"match": Map{"name": Map{"query": q.Keyword}}},
				{"match": Map{"desc": Map{"query": q.Keyword}}},
			}
			boolQuery["minimum_should_match"] = 1
		}

		filterTerms := []Map{}

		if len(q.CategoryIDs) > 0 {
			filterTerms = append(filterTerms, Map{"terms": Map{"category_id": q.CategoryIDs}})
		}
		if len(q.Statuses) > 0 {
			filterTerms = append(filterTerms, Map{"terms": Map{"status": q.Statuses}})
		}
		if len(filterTerms) > 0 {
			boolQuery["filter"] = filterTerms
		}

		for {
			esQuery := Map{
				"query": Map{
					"bool": boolQuery,
				},
				// "sort":    Map{"created": "desc"},
				"size":    size,
				"_source": fetchSource,
				"from":    from,
			}
			res, code, err := Call(http.MethodPost, ESHost+"/"+ESBenchIndexName+"/_search", ToJSON(esQuery))
			if err != nil {
				log.Panic(err)
			}
			if code >= 300 {
				fmt.Printf("Query dump:\n=====================\n%s\n", ToPrettyJSON(esQuery))
				log.Panicf("got unexpected status code %d : %s", code, res)
			}

			// preview := res[:]
			// if len(preview) > 500 {
			// 	preview = preview[0:500]
			// }
			// fmt.Printf("Preview: %s\n", preview)

			se := new(SearchResult)
			err = sonic.Unmarshal(res, se)
			if err != nil {
				log.Panic(err)
			}

			totalDocsFetched += len(se.Hits.Hits)

			if DebugPrint || qc%100 == 0 {
				fmt.Printf("Executed %d queries - fetched %d / %d (%s) item IDs\n", qc, totalDocsFetched, se.Hits.Total.Value, se.Hits.Total.Relation)
			}

			if fetchSource && se.Hits.Hits != nil {
				for i, doc := range se.Hits.Hits {
					fmt.Printf(
						"%03d. ID: %s  Name: %s  Status: %d  Category: %d\n", i+1,
						doc.Source.ID, doc.Source.Name, doc.Source.Status, doc.Source.CategoryID,
					)
					if i+1 >= 10 {
						break
					}
				}
			}

			hasNextPage := se.Hits.Total.Value > 0 && se.Hits.Total.Value > int64(size) && len(se.Hits.Hits) == int(size)
			if !hasNextPage || totalDocsFetched >= fetchMax {
				break
			}

			from += int64(len(se.Hits.Hits))
		}
	}
}

type SearchResult struct {
	Took int64 `json:"took"` // 2

	Hits struct {
		Total struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			Index  string  `json:"_index"` // "test"
			ID     string  `json:"_id"`    // "102"
			Score  float64 `json:"_score"` // 10.781843
			Source *Item   `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

var token *tokenizer.Tokenizer

func getTokenizer() *tokenizer.Tokenizer {
	if token == nil {
		var err error
		token, err = tokenizer.New(ipa.Dict(), tokenizer.OmitBosEos())
		if err != nil {
			log.Panic(err)
		}
	}
	return token
}

func ProcessItems(items []*Item) error {
	tok := getTokenizer()

	for _, i := range items {
		name := tok.Wakati(i.Name)
		i.Name = strings.Join(name, " ")

		desc := tok.Wakati(i.Desc)
		i.Desc = strings.Join(desc, " ")
	}

	var docs []interface{}
	for _, i := range items {
		docs = append(docs, Map{"index": Map{"_index": ESBenchIndexName, "_id": i.ID}})
		docs = append(docs, i)
	}

	bulk := BuildBulkBody(docs...)
	// fmt.Printf("bulk dump:\n%s\n\n", bulk)

	res, code, err := Call(http.MethodPost, ESHost+"/_bulk", bulk)
	EnsureNoError(res, code, err)

	return nil
}

func EnsureNoError(res []byte, statusCode int, err error) {
	if err != nil {
		log.Panic(err)
	}
	if !strings.Contains(string(res), `"errors":false`) {
		log.Panicf("got ES error: %s", res)
	}
	if statusCode != 200 {
		log.Panicf("got bad HTTP status code %d : %s", statusCode, res)
	}
}

func PrepareBenchIndex() {
	res, code, err := Call(http.MethodDelete, ESHost+"/"+ESBenchIndexName, nil)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}

	res, code, err = Call(http.MethodPut, ESHost+"/"+ESBenchIndexName, ToJSON(Map{
		"mappings": Map{
			"properties": Map{
				"id":          Map{"type": "keyword"},
				"name":        Map{"type": "text"},
				"desc":        Map{"type": "text"},
				"status":      Map{"type": "integer"},
				"created":     Map{"type": "date", "format": "epoch_millis"},
				"category_id": Map{"type": "integer"},
			},
		},
	}))
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}
}

func ReadItems(dataDir, filenameFilter string, batchSize, maxItemsToProcess int, forEachBatch func(items []*Item) error) {
	dir, err := os.ReadDir(dataDir)
	if err != nil {
		log.Panic(err)
	}

	var totalItems int
	var items []*Item

	for _, fi := range dir {
		if !strings.Contains(fi.Name(), filenameFilter) {
			continue
		}

		fmt.Printf("Reading Items file: %s\n", fi.Name())

		filename := filepath.Join(dataDir, fi.Name())
		f, err := os.Open(filename)
		if err != nil {
			log.Panic(err)
		}

		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Panic(err)
		}

		cr := csv.NewReader(gr)
		var lines int
		var exitEarly bool
		for {
			rec, err := cr.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Panic(err)
			}

			lines++
			if lines == 1 {
				// fmt.Printf("Headers: %+v\n", rec)
				continue
			}

			totalItems++

			if lines == 2 {
				preview := rec[1]
				if len(preview) > 30 {
					preview = preview[0:30]
				}
				fmt.Printf("Preview item: %s %s %s %s\n", rec[0], preview, rec[3], rec[4])
			}

			i := new(Item)
			i.ID = rec[0]
			i.Name = rec[1]
			i.Desc = rec[2]
			switch rec[3] {
			case "on_sale":
				i.Status = StatusOnSale
			case "trading":
				i.Status = StatusTrading
			case "sold_out":
				i.Status = StatusSold
			case "stop":
				i.Status = StatusStopped
			default:
				i.Status = StatusOther
			}
			i.Created = ToUnixTimestamp(rec[4])
			i.CategoryID = int(ToInt(rec[5]))

			items = append(items, i)
			if len(items) >= batchSize {
				forEachBatch(items)
				items = nil
			}

			if totalItems%50_000 == 0 {
				fmt.Printf("Processed %d items ..\n", totalItems)
			}

			if maxItemsToProcess > 0 && totalItems >= maxItemsToProcess {
				exitEarly = true
				break
			}
		}

		if len(items) > 0 {
			forEachBatch(items)
			items = nil
		}

		f.Close()
		if exitEarly {
			break
		}
	}

	fmt.Printf("Read %d items\n", totalItems)
}

func ToUnixTimestamp(s string) int64 {
	t, err := time.Parse("2006-01-02 15:04:05 MST", s)
	if err != nil {
		log.Panic(err)
	}
	return t.UnixMilli()
}

func ToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Panic(err)
	}
	return i
}

type Status int

const (
	StatusOnSale Status = iota + 1
	StatusTrading
	StatusSold
	StatusStopped
	StatusOther
)

type Item struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Desc       string `json:"desc"`
	Status     Status `json:"status"`
	Created    int64  `json:"created"`
	CategoryID int    `json:"category_id"`
}

func ESSanityTest() {
	res, code, err := Call(http.MethodDelete, ESHost+"/"+ESSanityTestIndexName, nil)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}

	res, code, err = Call(http.MethodPut, ESHost+"/"+ESSanityTestIndexName, ToJSON(Map{
		"mappings": Map{
			"properties": Map{
				"age":   Map{"type": "integer"},
				"email": Map{"type": "keyword"},
				"name":  Map{"type": "text"},
			},
		},
	}))
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}

	bulk := BuildBulkBody(
		Map{"index": Map{"_index": "test", "_id": "101"}},
		Map{"age": 30, "name": "Mr Magoo", "email": "mr@magoo.se"},
		Map{"index": Map{"_index": "test", "_id": "102"}},
		Map{"age": 25, "name": "Ms Molly", "email": "ms@molly.se"},
		Map{"index": Map{"_index": "test", "_id": "102"}},
		Map{"age": 21, "name": "Mrs Daisy Malone", "email": "dmalone@molly.se"},
	)
	// fmt.Printf("Bulk request:\n%s\n", string(bulk))

	res, code, err = Call(http.MethodPost, ESHost+"/_bulk", bulk)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}

	ESRefreshIndex(ESSanityTestIndexName)

	res, code, err = Call(http.MethodPost, ESHost+"/"+ESSanityTestIndexName+"/_search", ToJSON(
		Map{
			"query": Map{
				"query_string": Map{
					"query": `name:"daisy malone"^5 AND age:>=10^2`,
				},
			},
		},
	))
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}

	if !strings.Contains(string(res), "Mrs Daisy Malone") {
		log.Panicf("expected result to contain the string \"Mrs Daisy Malone\"")
	}

	fmt.Println("Connected to ES, created test index and executed a few queries - sanity test passed!")
}

type ESIndexStats struct {
	All struct {
		Primaries struct {
			Docs struct {
				Count int64 `json:"count"`
			} `json:"docs"`
			Store struct {
				SizeInBytes             int64 `json:"size_in_bytes"`
				TotalDataSetSizeInBytes int64 `json:"total_data_set_size_in_bytes"`
			} `json:"store"`
		} `json:"primaries"`
	} `json:"_all"`
}

func ESGetIndexStats(index string) *ESIndexStats {
	res, _, err := Call(http.MethodGet, ESHost+"/"+index+"/_stats", nil)
	if err != nil {
		log.Panic(err)
	}

	stats := new(ESIndexStats)
	err = sonic.Unmarshal(res, stats)
	if err != nil {
		log.Panic(err)
	}

	return stats
}

func ESRefreshIndex(index string) {
	res, code, err := Call(http.MethodGet, ESHost+"/"+index+"/_refresh", nil)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}
}

func BuildBulkBody(obs ...interface{}) (bulk []byte) {
	for _, o := range obs {
		bulk = append(bulk, ToJSON(o)...)
		bulk = append(bulk, []byte("\n")...)
	}
	bulk = append(bulk, []byte("\n")...)
	return
}

func ToJSON(o interface{}) []byte {
	b, err := sonic.Marshal(o)
	if err != nil {
		log.Panic(err)
	}
	return b
}

func ToPrettyJSON(o interface{}) []byte {
	b, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		log.Panic(err)
	}
	return b
}

func Call(method, url string, body []byte) (respBody []byte, statusCode int, err error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return nil, 0, err
	}

	req.Header.Add("content-type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	statusCode = resp.StatusCode

	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return
}
