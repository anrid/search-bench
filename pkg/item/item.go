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
	StatusCancel
	StatusOther
)

type ItemCondition int

const (
	ItemConditionLikeNew ItemCondition = iota + 1
	ItemConditionGood
	ItemConditionPoor
	ItemConditionOther
)

type Item struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Desc       string `json:"desc"`
	Status     Status `json:"status"`
	Created    int64  `json:"created"`
	CategoryID int    `json:"category_id"`
}

type ItemNoDesc struct {
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	Status        Status        `json:"status"`
	Created       int64         `json:"created"`
	Updated       int64         `json:"updated"`
	CategoryID    int           `json:"category_id"`
	Price         int           `json:"price"`
	ItemCondition ItemCondition `json:"item_condition"`
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
	var updates, inserts int

	itemsToImport := a.StartFrom + a.MaxItems
	maxUpdates := a.MaxItems / 3
	maxInserts := (a.MaxItems / 3) * 2

	createChangeLogBatch := func(totalItems int, items []*Item) error {
		itemNumber := totalItems - len(items)

		for _, i := range items {
			itemNumber++

			if itemNumber <= a.StartFrom {
				// Create update event
				if updates >= maxUpdates {
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
				updates++
			} else {
				// Create insert event
				if inserts >= maxInserts {
					return fmt.Errorf("done after creating a change log with %d updates and %d inserts", updates, inserts)
				}

				changeLog = append(changeLog, &ChangeLogEntry{
					ItemID: i.ID,
					Insert: i,
				})
				inserts++
			}
		}

		return nil
	}

	batcher := &ItemsBatch{
		Size:         a.BatchSize,
		ForEachBatch: createChangeLogBatch,
	}

	fmt.Printf("Creating a new change log with max %d updates and %d inserts\n", maxUpdates, maxInserts)

	Import(ImportArgs{
		DataDir:          a.DataDir,
		FilenameFilter:   a.FilenameFilter,
		Batcher:          batcher,
		MaxItemsToImport: itemsToImport,
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

	fmt.Printf(
		"Wrote change log with %d updates and %d inserts to file: %s (%d bytes)\n",
		updates, inserts, a.ChangeLogFile, len(b),
	)

	return
}

type ImportArgs struct {
	DataDir          string
	FilenameFilter   string
	MaxItemsToImport int
	Batcher          Batcher
}

func Import(a ImportArgs) {
	dir, err := os.ReadDir(a.DataDir)
	if err != nil {
		log.Panic(err)
	}

	var total int

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
		var headers []string

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
				fmt.Printf("Headers: %+v\n", rec)
				headers = rec
				continue
			}

			total++

			err = a.Batcher.Add(rec, headers)
			if err != nil {
				log.Panic(err)
			}

			if total%10_000 == 0 {
				fmt.Printf("Processed %d records ..\n", total)
			}

			if a.MaxItemsToImport > 0 && total >= a.MaxItemsToImport {
				exitEarly = true
				break
			}
		}

		err = a.Batcher.Flush()
		if err != nil {
			log.Panic(err)
		}

		f.Close()
		if exitEarly {
			break
		}
	}

	fmt.Printf("Imported %d items total\n", total)
}

type ItemsBatch struct {
	Size         int
	Total        int
	Items        []*Item
	ForEachBatch func(totalItems int, items []*Item) error
}

func (b *ItemsBatch) Add(rec, headers []string) error {
	isItem := len(rec) == 6 && headers[2] == "description"
	if !isItem {
		return fmt.Errorf("does not look like an Item record: %+v", headers)
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
	i.CategoryID = int(data.ToInt64(rec[5]))

	b.Total++

	if b.Total == 1 {
		fmt.Printf("Preview item: %v\n", rec)
	}

	b.Items = append(b.Items, i)

	if len(b.Items) >= b.Size {
		err := b.ForEachBatch(b.Total, b.Items)
		if err != nil {
			return err
		}
		b.Items = nil
	}
	return nil
}

func (b *ItemsBatch) Flush() error {
	if len(b.Items) > 0 {
		err := b.ForEachBatch(b.Total, b.Items)
		if err != nil {
			return err
		}
		b.Items = nil
	}
	return nil
}

type ItemsNoDescBatch struct {
	Size         int
	Total        int
	Items        []*ItemNoDesc
	ForEachBatch func(totalItems int, items []*ItemNoDesc) error
}

func (b *ItemsNoDescBatch) Add(rec, headers []string) error {
	isItem := len(rec) == 8 && headers[2] == "status"
	if !isItem {
		return fmt.Errorf("does not look like an ItemNoDesc record: %+v", headers)
	}

	i := new(ItemNoDesc)

	i.ID = rec[0]
	i.Name = rec[1]
	switch rec[2] {
	case "on_sale":
		i.Status = StatusOnSale
	case "trading":
		i.Status = StatusTrading
	case "sold_out":
		i.Status = StatusSold
	case "stop":
		i.Status = StatusStopped
	case "cancel":
		i.Status = StatusCancel
	default:
		i.Status = StatusOther
	}
	i.Created = data.ToInt64(rec[3])
	i.Updated = data.ToInt64(rec[4])
	i.CategoryID = int(data.ToInt64(rec[5]))
	i.Price = int(data.ToInt64(rec[6]))
	switch rec[7] {
	case "1":
		i.ItemCondition = ItemConditionLikeNew
	case "2":
		i.ItemCondition = ItemConditionGood
	case "3":
		i.ItemCondition = ItemConditionPoor
	default:
		i.ItemCondition = ItemConditionOther
	}

	b.Total++

	if b.Total <= 10 {
		fmt.Printf("Preview item: %v\n", rec)
	}

	b.Items = append(b.Items, i)

	if len(b.Items) >= b.Size {
		err := b.ForEachBatch(b.Total, b.Items)
		if err != nil {
			return err
		}
		b.Items = nil
	}
	return nil
}

func (b *ItemsNoDescBatch) Flush() error {
	if len(b.Items) > 0 {
		err := b.ForEachBatch(b.Total, b.Items)
		if err != nil {
			return err
		}
		b.Items = nil
	}
	return nil
}

type Batcher interface {
	Add(rec, headers []string) error
	Flush() error
}
