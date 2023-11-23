package query

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/anrid/search-bench/pkg/data"
	"github.com/anrid/search-bench/pkg/item"
	"github.com/bytedance/sonic"
)

type SearchQuery struct {
	Keyword     string
	CategoryIDs []int64
	Statuses    []item.Status
}

type RawSearchQuery struct {
	Query string `json:"query"`
	Count string `json:"c"`
}

func Load(queriesFile string) (qs []*SearchQuery) {
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

	tok := data.KagomeV2Tokenizer()

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
				q.CategoryIDs = append(q.CategoryIDs, data.ToInt(cat))
			}
		}

		if len(parts[2]) > 2 {
			// Handle statuses array in JSON string format
			trimmed := parts[2][1 : len(parts[2])-1]
			statuses := strings.SplitN(trimmed, ",", -1)
			for _, status := range statuses {
				switch status {
				case "ITEM_STATUS_ON_SALE":
					q.Statuses = append(q.Statuses, item.StatusOnSale)
				case "ITEM_STATUS_TRADING":
					q.Statuses = append(q.Statuses, item.StatusTrading)
				case "ITEM_STATUS_SOLD_OUT":
					q.Statuses = append(q.Statuses, item.StatusSold)
				}
			}
		}

		qs = append(qs, q)
	}

	fmt.Printf("Loaded and prepared %d queries\n", len(qs))

	return
}
