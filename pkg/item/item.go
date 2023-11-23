package item

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/anrid/search-bench/pkg/data"
)

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

type ImportArgs struct {
	DataDir          string
	FilenameFilter   string
	BatchSize        int
	MaxItemsToImport int
	ForEachBatch     func(items []*Item) error
}

func Import(a ImportArgs) {
	dir, err := os.ReadDir(a.DataDir)
	if err != nil {
		log.Panic(err)
	}

	var totalItems int
	var items []*Item

	for _, fi := range dir {
		if !strings.Contains(fi.Name(), a.FilenameFilter) {
			continue
		}

		fmt.Printf("Importing items from file: %s\n", fi.Name())

		filename := filepath.Join(a.DataDir, fi.Name())
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
			i.Created = data.ToUnixTimestamp(rec[4])
			i.CategoryID = int(data.ToInt(rec[5]))

			items = append(items, i)
			if len(items) >= a.BatchSize {
				a.ForEachBatch(items)
				items = nil
			}

			if totalItems%50_000 == 0 {
				fmt.Printf("Processed %d items ..\n", totalItems)
			}

			if a.MaxItemsToImport > 0 && totalItems >= a.MaxItemsToImport {
				exitEarly = true
				break
			}
		}

		if len(items) > 0 {
			a.ForEachBatch(items)
			items = nil
		}

		f.Close()
		if exitEarly {
			break
		}
	}

	fmt.Printf("Imported %d items total\n", totalItems)
}
