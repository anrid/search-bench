package item

import (
	"compress/gzip"
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/big"
	mrand "math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

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

type CreateChangeLogArgs struct {
	ChangeLogFile  string
	DataDir        string
	FilenameFilter string
	BatchSize      int
	StartFrom      int
	MaxItems       int
	ForEachBatch   func(items []*Item) error
}

type ChangeLogEntry struct {
	ItemID string
	Update struct {
		Name    string `json:"name,omitempty"`
		Created int64  `json:"created,omitempty"`
		Status  Status `json:"status,omitempty"`
	} `json:"update,omitempty"`
	Insert *Item `json:"insert,omitempty"`
}

func CreateChangeLog(a CreateChangeLogArgs) (changeLog []*ChangeLogEntry) {
	changeLog = make([]*ChangeLogEntry, 0)

	Import(ImportArgs{
		DataDir:          a.DataDir,
		FilenameFilter:   a.FilenameFilter,
		BatchSize:        a.BatchSize,
		MaxItemsToImport: a.MaxItems,
		ForEachBatch: func(totalItems int, items []*Item) error {
			itemNumber := totalItems - len(items)

			// Use ~33% of items with item numbers <= a.StartFrom for
			// update events, e.g. to simulate updates on `created` and
			// `status` fields
			maxItemNumberForUpdate := int(float64(len(items)) * 0.33)

			for _, i := range items {
				itemNumber++

				if itemNumber <= a.StartFrom {
					// Create update event
					if itemNumber > maxItemNumberForUpdate {
						// Skip 67% of all items
						continue
					}

					// Create random update
					rnd, err := rand.Int(rand.Reader, big.NewInt(10))
					if err != nil {
						log.Panic(err)
					}

					cl := &ChangeLogEntry{
						ItemID: i.ID,
					}

					switch r := rnd.Int64(); {
					case r >= 0 && r < 3:
						// 30% chance of update on `created` field
						cl.Update.Created = time.UnixMilli(i.Created).Add(24 * time.Hour).UnixMilli()
					case r >= 3:
						// 70% chance of update on `status` field
						if i.Status == StatusOnSale || i.Status == StatusTrading {
							cl.Update.Status = StatusSold
						}
					}

					changeLog = append(changeLog, cl)
				} else {
					// Create insert event
					changeLog = append(changeLog, &ChangeLogEntry{
						ItemID: i.ID,
						Insert: i,
					})
				}
			}

			return nil
		},
	})

	// Shuffle change log!
	mrand.Shuffle(len(changeLog), func(i, j int) {
		changeLog[i], changeLog[j] = changeLog[j], changeLog[i]
	})

	b := data.ToJSON(changeLog)
	err := os.WriteFile(a.ChangeLogFile, b, 0777)
	if err != nil {
		log.Panic(err)
	}

	fmt.Printf("Wrote %d bytes to change log file: %s\n", len(b), a.ChangeLogFile)

	return
}

type ImportArgs struct {
	DataDir          string
	FilenameFilter   string
	BatchSize        int
	MaxItemsToImport int
	ForEachBatch     func(totalItems int, items []*Item) error
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
				err = a.ForEachBatch(totalItems, items)
				if err != nil {
					break
				}
				items = nil
			}

			if totalItems%10_000 == 0 {
				fmt.Printf("Processed %d items ..\n", totalItems)
			}

			if a.MaxItemsToImport > 0 && totalItems >= a.MaxItemsToImport {
				exitEarly = true
				break
			}
		}

		if len(items) > 0 {
			a.ForEachBatch(totalItems, items)
			items = nil
		}

		f.Close()
		if exitEarly {
			break
		}
	}

	fmt.Printf("Imported %d items total\n", totalItems)
}
