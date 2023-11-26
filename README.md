# Search Bench

## Results at a glance

| Search Engine                | ES 7.16.2 | ES 7.17.15 | ES 8.11.1 | Best vs Worst |
| ---------------------------- | --------- | ---------- | --------- | ------------- |
| Index time (1M items)        | 5m 10s    | 4m 48s     | 4m 34s    | -12%          |
| Avg search time (1k queries) | 21.26s    | 19.04s     | 9.90s     | -54%          |

### Search results comparison (BM25 b=0.75, k1=1.2)

- ES **8.11.1** vs **7.17.15** : `10.04%` of search results differ (`1.53%` of primary keys different on average)
- ES **8.11.1** vs **7.16.2** : `9.72%` of search results differ (`1.53%` of primary keys different on average)
- ES **7.17.15** vs **7.16.2** : `3.96%` of search results differ (`0.00%` of primary keys different on average)

## Data used in all benchmarks

- CSV files containing items from a popular second-hand marketplace
  - Format:
    - ID (`string`) : Primary key
    - Name (`string`) : Title/name of the item, mainly Japanese text (max 255 glyphs)
    - Desc (`string`) : Description/body of the item, mainly Japanese text (can be many pages of text)
    - Status (`int`) : `1` = On Sale, `2` = Trading, `3` = Sold
    - Created (`int64`) : Created date as millisec timestamp
    - CategoryID (`int`) : One out of 1,200+ categories
- [Kagome V2](https://github.com/ikawaha/kagome/tree/v2) is used to tokenize Japanese text

## ES 7.x vs 8.x

- All calls to Elasticsearch is done using raw REST API calls
- Benchmark run on a modern desktop PC running Windows 11
  - Spec:
    - Processor 12th Gen Intel(R) Core(TM) i7-12700KF 3.61 GHz
    - Installed RAM 32.0 GB (31.8 GB usable)
- Elasticsearch is run in `single-node mode` via Docker for Windows
- See [examples of queries used](#examples-of-elastichsearch-queries-used)

### Elasticsearch 7.17.15

```bash
# Elasticsearch 7.17.15
$ docker start es7

# Index 1,000,000 items
$ go run cmd/cli/main.go --run-indexer --data-dir ../data --batch-size 5_000 --max 1_000_000

Connected to ES, created test index and executed a few queries - sanity test passed!
Running indexer: max 1000000 items ..
Importing items from file: items-1week-0001.csv.gz
Preview item: id1 Zippo Lighter(未使用) on_sale 2023-11-21 23:02:53 UTC
Bulk indexing 5000 items (JSON payload: 7783448 bytes)
Bulk indexing 5000 items (JSON payload: 7590630 bytes)
Processed 10000 items ..

...

Bulk indexing 1016 items (JSON payload: 1272289 bytes)
Imported 1000000 items total
Index stats (after):
{
  "_all": {
    "primaries": {
      "docs": {
        "count": 1000000
      },
      "store": {
        "size_in_bytes": 1195189251,
        "total_data_set_size_in_bytes": 1195189251
      },
  }
}
Finished indexing 1000000 items in 4m48.981955219s

# Run query benchmark
$ go run cmd/cli/main.go -q ../top-1000-queries.json --runs 5

Connected to ES, created test index and executed a few queries - sanity test passed!
Loading queries from ../top-1000-queries.json ..
Loaded and prepared 1000 queries
Index stats (before):
{
  "_all": {
    "primaries": {
      "docs": {
        "count": 1000000
      },
      "store": {
        "size_in_bytes": 1195189251,
        "total_data_set_size_in_bytes": 1195189251
      },
      "query_cache": {
        "cache_count": 0,
        "cache_size": 0,
        "evictions": 0,
        "hit_count": 0,
        "memory_size_in_bytes": 0,
        "miss_count": 0,
        "total_count": 0
      },
      "request_cache": {
        "evictions": 0,
        "hit_count": 0,
        "memory_size_in_bytes": 0,
        "miss_count": 0
      }
    }
  }
}

Executed 100 queries - fetched 120 / 1833 (eq) item IDs
Executed 100 queries - fetched 240 / 1833 (eq) item IDs
Executed 200 queries - fetched 120 / 1160 (eq) item IDs
Executed 200 queries - fetched 240 / 1160 (eq) item IDs

...

Executed 1000 queries x 5 runs. Average time 19.050047411s
Index stats (after):
{
  "_all": {
    "primaries": {
      "docs": {
        "count": 1000000
      },
      "store": {
        "size_in_bytes": 1195189251,
        "total_data_set_size_in_bytes": 1195189251
      },
      "query_cache": {
        "cache_count": 0,
        "cache_size": 0,
        "evictions": 0,
        "hit_count": 0,
        "memory_size_in_bytes": 0,
        "miss_count": 0,
        "total_count": 0
      },
      "request_cache": {
        "evictions": 0,
        "hit_count": 0,
        "memory_size_in_bytes": 0,
        "miss_count": 0
      }
    }
  }
}

```

#### Elasticsearch 7.17.15 Results

- Indexed 1,000,000 docs in `4m 49s`
- Total index size `1,195,189,251 bytes`
- Executed 1,000 popular queries in `5 runs` at an average time of `19.05s`
  - Both query and request caches disabled

### Elasticsearch 8.11.1

```bash
# Elasticsearch 8.11.1
$ docker start es8

# Index 1,000,000 items
$ go run cmd/cli/main.go --run-indexer --data-dir ../data --batch-size 5_000 --max 1_000_000

Connected to ES, created test index and executed a few queries - sanity test passed!
Running indexer: max 1000000 items ..
Importing items from file: items-1week-0001.csv.gz
Preview item: id1 Zippo Lighter(未使用) on_sale 2023-11-21 23:02:53 UTC
Bulk indexing 5000 items (JSON payload: 7783448 bytes)
Bulk indexing 5000 items (JSON payload: 7590630 bytes)
Processed 10000 items ..

...

Bulk indexing 1016 items (JSON payload: 1272289 bytes)
Imported 1000000 items total
Index stats (after):
{
  "_all": {
    "primaries": {
      "docs": {
        "count": 1000000
      },
      "store": {
        "size_in_bytes": 1190922960,
        "total_data_set_size_in_bytes": 1190922960
      },
  }
}
Finished indexing 1000000 items in 4m30.898442043s

# Run query benchmark
$ go run cmd/cli/main.go -q ../top-1000-queries.json --runs 5

Connected to ES, created test index and executed a few queries - sanity test passed!
Loading queries from ../top-1000-queries.json ..
Loaded and prepared 1000 queries
Index stats (before):
{
  "_all": {
    "primaries": {
      "docs": {
        "count": 1000000
      },
      "store": {
        "size_in_bytes": 1190922960,
        "total_data_set_size_in_bytes": 1190922960
      },
      "query_cache": {
        "cache_count": 0,
        "cache_size": 0,
        "evictions": 0,
        "hit_count": 0,
        "memory_size_in_bytes": 0,
        "miss_count": 0,
        "total_count": 0
      },
      "request_cache": {
        "evictions": 0,
        "hit_count": 0,
        "memory_size_in_bytes": 0,
        "miss_count": 0
      }
    }
  }
}

Executed 100 queries - fetched 120 / 1833 (eq) item IDs
Executed 100 queries - fetched 240 / 1833 (eq) item IDs
Executed 200 queries - fetched 120 / 1160 (eq) item IDs
Executed 200 queries - fetched 240 / 1160 (eq) item IDs

...

Executed 1000 queries x 5 runs. Average time 9.903988107s
Index stats (after):
{
  "_all": {
    "primaries": {
      "docs": {
        "count": 1000000
      },
      "store": {
        "size_in_bytes": 1190922960,
        "total_data_set_size_in_bytes": 1190922960
      },
      "query_cache": {
        "cache_count": 0,
        "cache_size": 0,
        "evictions": 0,
        "hit_count": 0,
        "memory_size_in_bytes": 0,
        "miss_count": 0,
        "total_count": 0
      },
      "request_cache": {
        "evictions": 0,
        "hit_count": 0,
        "memory_size_in_bytes": 0,
        "miss_count": 0
      }
    }
  }
}

```

### Elasticsearch 8.11.1 Results

- Indexed 1,000,000 docs in `4m 31s`
- Total index size `1,190,922,960 bytes`
- Executed 1,000 popular queries in `5 runs` at an average time of `9.90s`
  - Both query and request caches disabled

> [!NOTE]
>
> - Query execution is `~32%` faster than ES version `7.17.15`

### Examples of Elasticsearch queries used

```json
// category_id only
{
  "_source": false,
  "from": 0,
  "query": {
    "bool": {
      "filter": [
        {
          "terms": {
            "category_id": [
              344
            ]
          }
        }
      ]
    }
  },
  "size": 120,
  "sort": {
    "created": "desc"
  }
}

// keyword only
{
  "_source": false,
  "from": 120,
  "query": {
    "bool": {
      "minimum_should_match": 1,
      "should": [
        {
          "match": {
            "name": {
              "query": "エヴァ   バッジ"
            }
          }
        },
        {
          "match": {
            "desc": {
              "query": "エヴァ   バッジ"
            }
          }
        }
      ]
    }
  },
  "size": 120,
  "sort": {
    "_score": "desc"
  }
}

// Both status and category_id
{
  "_source": false,
  "from": 120,
  "query": {
    "bool": {
      "filter": [
        {
          "terms": {
            "category_id": [
              174
            ]
          }
        },
        {
          "terms": {
            "status": [
              1
            ]
          }
        }
      ]
    }
  },
  "size": 120,
  "sort": {
    "created": "desc"
  }
}

// Both keyword and category_id
{
  "_source": false,
  "from": 0,
  "query": {
    "bool": {
      "filter": [
        {
          "terms": {
            "category_id": [
              3
            ]
          }
        }
      ],
      "minimum_should_match": 1,
      "should": [
        {
          "match": {
            "name": {
              "query": "冬"
            }
          }
        },
        {
          "match": {
            "desc": {
              "query": "冬"
            }
          }
        }
      ]
    }
  },
  "size": 120,
  "sort": {
    "_score": "desc"
  }
}

```
