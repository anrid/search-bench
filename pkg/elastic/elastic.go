package elastic

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/anrid/search-bench/pkg/data"
	"github.com/anrid/search-bench/pkg/item"
	"github.com/anrid/search-bench/pkg/query"
	"github.com/bytedance/sonic"
)

const (
	Host                = "http://127.0.0.1:9200"
	SanityTestIndexName = "test"
	ItemIndexName       = "items"
	DebugPrint          = false
)

type Map = map[string]interface{}

type RunIndexerArgs struct {
	DataDir        string
	FilenameFilter string
	BatchSize      int
	Max            int
}

func RunIndexer(a RunIndexerArgs) {
	CreateItemIndex()

	start := time.Now()

	item.Import(item.ImportArgs{
		DataDir:          a.DataDir,
		FilenameFilter:   a.FilenameFilter,
		BatchSize:        a.BatchSize,
		MaxItemsToImport: a.Max,
		ForEachBatch:     BulkIndexItems,
	})

	Refresh(ItemIndexName)

	stats := IndexStats(ItemIndexName)
	fmt.Printf("Index stats (after):\n%s\n", data.ToPrettyJSON(stats))

	fmt.Printf("Finished indexing %d items in %s\n", stats.All.Primaries.Docs.Count, time.Since(start))
}

func RunBenchmark(runs int, queries []*query.SearchQuery, fetchSource bool) {
	statsBefore := IndexStats(ItemIndexName)
	fmt.Printf("Index stats (before):\n%s\n", data.ToPrettyJSON(statsBefore))

	var totalDuration time.Duration

	for run := 0; run < runs; run++ {
		runStart := time.Now()
		ExecuteQueries(queries, fetchSource, 240)
		totalDuration += time.Since(runStart)
	}

	fmt.Printf("Executed %d queries x %d runs. Average time %s\n", len(queries), runs, totalDuration/time.Duration(runs))

	statsAfter := IndexStats(ItemIndexName)
	fmt.Printf("Index stats (after):\n%s\n", data.ToPrettyJSON(statsAfter))
}

func ExecuteQueries(queries []*query.SearchQuery, fetchSource bool, fetchMax int) {
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
			res, code, err := Call(http.MethodPost, Host+"/"+ItemIndexName+"/_search?request_cache=false", data.ToJSON(esQuery))
			if err != nil {
				log.Panic(err)
			}
			if code >= 300 {
				fmt.Printf("Query dump:\n=====================\n%s\n", data.ToPrettyJSON(esQuery))
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
			Index  string     `json:"_index"` // "test"
			ID     string     `json:"_id"`    // "102"
			Score  float64    `json:"_score"` // 10.781843
			Source *item.Item `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func BulkIndexItems(itemsTotal int, items []*item.Item) error {
	tok := data.KagomeV2Tokenizer()

	for _, i := range items {
		name := tok.Wakati(i.Name)
		i.Name = strings.Join(name, " ")

		desc := tok.Wakati(i.Desc)
		i.Desc = strings.Join(desc, " ")
	}

	var docs []interface{}
	for _, i := range items {
		docs = append(docs, Map{"index": Map{"_index": ItemIndexName, "_id": i.ID}})
		docs = append(docs, i)
	}

	bulk := BuildBulkBody(docs...)
	if len(bulk) > 10_000_000 {
		fmt.Printf("WARNING: bulk index body is %d bytes large!\n", len(bulk))
	}

	fmt.Printf("Bulk indexing %d items (JSON payload: %d bytes)\n", len(items), len(bulk))
	res, code, err := Call(http.MethodPost, Host+"/_bulk", bulk)
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

func CreateItemIndex() {
	res, code, err := Call(http.MethodDelete, Host+"/"+ItemIndexName, nil)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}

	res, code, err = Call(http.MethodPut, Host+"/"+ItemIndexName, data.ToJSON(Map{
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
		"settings": Map{
			"index": Map{
				"queries.cache.enabled": "false",
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

func SanityTest() {
	res, code, err := Call(http.MethodDelete, Host+"/"+SanityTestIndexName, nil)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}

	res, code, err = Call(http.MethodPut, Host+"/"+SanityTestIndexName, data.ToJSON(Map{
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

	res, code, err = Call(http.MethodPost, Host+"/_bulk", bulk)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}

	Refresh(SanityTestIndexName)

	res, code, err = Call(http.MethodPost, Host+"/"+SanityTestIndexName+"/_search", data.ToJSON(
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
			QueryCache struct {
				CacheCount     int64 `json:"cache_count"`
				CacheSize      int64 `json:"cache_size"`
				Evictions      int64 `json:"evictions"`
				HitCount       int64 `json:"hit_count"`
				MemSizeInBytes int64 `json:"memory_size_in_bytes"`
				MissCount      int64 `json:"miss_count"`
				TotalCount     int64 `json:"total_count"`
			} `json:"query_cache"`
			RequestCache struct {
				Evictions      int64 `json:"evictions"`
				HitCount       int64 `json:"hit_count"`
				MemSizeInBytes int64 `json:"memory_size_in_bytes"`
				MissCount      int64 `json:"miss_count"`
			} `json:"request_cache"`
		} `json:"primaries"`
	} `json:"_all"`
}

func IndexStats(index string) *ESIndexStats {
	res, _, err := Call(http.MethodGet, Host+"/"+index+"/_stats", nil)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		all := make(map[string]interface{})
		err = sonic.Unmarshal(res, &all)
		if err != nil {
			log.Panic(err)
		}
		fmt.Printf("All Stats:\n%s\n\n", data.ToPrettyJSON(all))
	}

	stats := new(ESIndexStats)
	err = sonic.Unmarshal(res, stats)
	if err != nil {
		log.Panic(err)
	}

	return stats
}

func Refresh(index string) {
	res, code, err := Call(http.MethodGet, Host+"/"+index+"/_refresh", nil)
	if err != nil {
		log.Panic(err)
	}

	if DebugPrint {
		fmt.Printf("res: %s (code: %d)\n", res, code)
	}
}

func BuildBulkBody(obs ...interface{}) (bulk []byte) {
	for _, o := range obs {
		bulk = append(bulk, data.ToJSON(o)...)
		bulk = append(bulk, []byte("\n")...)
	}
	bulk = append(bulk, []byte("\n")...)
	return
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
