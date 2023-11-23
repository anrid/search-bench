# Search Bench

### Data used:

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

```bash
# Elasticsearch 7.17.15
$ docker start es7

# Run benchmark
$ go run cmd/cli/main.go --data-dir ../data --batch-size 5000 --queries-file ../queries.json --run-indexer --max 500_000

Loading queries from ../queries.json ..
Loaded and prepared 1000 queries
Connected to ES, created test index and executed a few queries - sanity test passed!
Reading Items file: items-0001.csv.gz
Processed 50000 items ..
...
Reading Items file: items-0003.csv.gz
Processed 450000 items ..
Processed 500000 items ..
Read 500000 items
Finished indexing 500000 items in 2m39.436819284s
Index stats:
{
  "_all": {
    "primaries": {
      "docs": {
        "count": 500000
      },
      "store": {
        "size_in_bytes": 617327009,
        "total_data_set_size_in_bytes": 617327009
      }
    }
  }
}

Executed 1000 queries x 3 runs. Average time 11.37215205s

```

### Elasticsearch 7.17.15 Results

- Indexed 500,000 docs in `2m 40s`
- Total index size `617,327,009 bytes`
- Executed 1,000 popular queries in `11.37s`

```bash
# Elasticsearch 8.11.1
$ docker start es8

# Run benchmark
$ go run cmd/cli/main.go --data-dir ../data --batch-size 5000 --queries-file ../queries.json --run-indexer --max 500_000

Loading queries from ../queries.json ..
Loaded and prepared 1000 queries
Connected to ES, created test index and executed a few queries - sanity test passed!
Reading Items file: items-0001.csv.gz
Processed 50000 items ..
...
Reading Items file: items-0003.csv.gz
Processed 450000 items ..
Processed 500000 items ..
Read 500000 items
Finished indexing 500000 items in 2m46.600602911s
Index stats:
{
  "_all": {
    "primaries": {
      "docs": {
        "count": 500000
      },
      "store": {
        "size_in_bytes": 615131259,
        "total_data_set_size_in_bytes": 615131259
      }
    }
  }
}

Executed 1000 queries x 3 runs. Average time 8.370502153s

```

### Elasticsearch 8.11.1 Results

- Indexed 500,000 docs in `2m 46s`
- Total index size `615,131,259 bytes`
- Executed 1,000 popular queries in `8.37s` (`~24%` faster than ES version `7.17.15`)
