package compare

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	DebugPrint = false
)

type Line struct {
	FromFile string
	Text     string
}

func ReadFilesLineByLine(files []string, forEachLine func(lineNumber int, lines []*Line) error) {
	var names []string

	// Open all results files
	var fs []*os.File
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Panic(err)
		}
		fs = append(fs, f)
		defer f.Close()

		names = append(names, filepath.Base(f.Name()))
	}

	// Read results from all files line-by-line
	var scans []*bufio.Scanner
	for _, f := range fs {
		scans = append(scans, bufio.NewScanner(f))
	}

	var lines []*Line
	var lineNumber int

	for {
		var end int

		for i, s := range scans {
			if s.Scan() {
				text := strings.Trim(s.Text(), "\n\t\r ")
				if text == "" {
					end++
					break
				} else {
					lines = append(lines, &Line{
						FromFile: names[i],
						Text:     text,
					})
				}
			} else {
				end++
			}
		}

		if end > 0 {
			break
		}

		lineNumber++

		err := forEachLine(lineNumber, lines)
		if err != nil {
			break
		}

		lines = nil
	}
}

func CompareResults(resultsFiles []string) {
	var identical, different, total int
	var diffRatio, diffRatioCount float64

	ReadFilesLineByLine(resultsFiles, func(lineNumber int, lines []*Line) error {
		if len(lines) < 2 {
			log.Panic("expected to compare lines between at least 2 files")
		}

		main := lines[0]
		others := lines[1:]

		parts := strings.SplitN(main.Text, "|", 2)
		queryNumber := parts[0]
		mainIDs := strings.SplitN(parts[1], ",", -1)

		for _, o := range others {
			parts = strings.SplitN(o.Text, "|", 2)
			otherQueryNumber := parts[0]

			if queryNumber != otherQueryNumber {
				log.Panicf(
					"found query #%s in file %s but query #%s in file %s on the same line",
					queryNumber, main.FromFile,
					otherQueryNumber, o.FromFile,
				)
			}

			otherIDs := strings.SplitN(parts[1], ",", -1)

			if !Equal(mainIDs, otherIDs) {
				different++

				onlyA, onlyB, _ := Diff(mainIDs, otherIDs)

				if DebugPrint {
					if len(onlyA) > 0 {
						fmt.Printf(
							"#%-5s %-20s IDs found only in main  : %3d\n",
							queryNumber, main.FromFile, len(onlyA),
						)
					}
					if len(onlyB) > 0 {
						fmt.Printf(
							"#%-5s %-20s IDs found only in other : %3d \n",
							queryNumber, o.FromFile, len(onlyB),
						)
					}
				}

				if len(onlyA) > 0 && len(onlyB) == len(onlyA) && len(mainIDs) == len(otherIDs) {
					diffRatio += float64(len(onlyA)) / float64(len(mainIDs))
					diffRatioCount++
				}

			} else {
				identical++
			}
			total++
		}

		// if queryNumber == "100" {
		// 	return fmt.Errorf("existing early at query number %s", queryNumber)
		// }

		return nil
	})

	var averageDiffPct float64
	if diffRatioCount > 0 {
		averageDiffPct = (diffRatio / diffRatioCount) * 100
	}

	fmt.Printf(
		"\n"+
			"Comparison:\n"+
			"        -  total results compared : %d\n"+
			"        -  identical results      : %d\n"+
			"        -  different results      : %d\n"+
			"        -  percentage that differ : %0.2f%%\n"+
			"\n"+
			"           average diff between results\n"+
			"           (primary keys that appear in one\n"+
			"            result but not the other)        : %.02f%%\n",
		total,
		identical,
		different,
		(float64(different)/float64(total))*100.00,
		averageDiffPct,
	)
}

func Equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func Diff(a, b []string) (onlyA, onlyB, both []string) {
	am := make(map[string]bool)
	bm := make(map[string]bool)

	for _, v := range a {
		am[v] = true
	}
	for _, v := range b {
		bm[v] = true
	}

	for aK := range am {
		if !bm[aK] {
			onlyA = append(onlyA, aK)
		} else {
			both = append(both, aK)
		}
	}
	for bK := range bm {
		if !am[bK] {
			onlyB = append(onlyB, bK)
		}
	}

	return
}
