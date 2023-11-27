package compare

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/anrid/search-bench/pkg/data"
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

func CompareResults(fileA, fileB string) {
	stats := struct {
		SortByDate struct {
			Identical                int
			Different                int
			Total                    int
			DiffPct                  float64
			PrimaryKeyDiffRatio      float64
			PrimaryKeyDiffRatioCount float64
			PrimaryKeyAvgDiffPct     float64
		}
		Bestmatch struct {
			Identical                int
			Different                int
			Total                    int
			DiffPct                  float64
			PrimaryKeyDiffRatio      float64
			PrimaryKeyDiffRatioCount float64
			PrimaryKeyAvgDiffPct     float64
		}
	}{}

	ReadFilesLineByLine([]string{fileA, fileB}, func(lineNumber int, lines []*Line) error {
		if len(lines) != 2 {
			log.Panic("expected to compare lines from 2 files")
		}

		lineA := lines[0]
		lineB := lines[1]

		pA := strings.SplitN(lineA.Text, "|", 3)
		queryNumberA := pA[0]
		isBestmatchA := false
		if pA[1] == "bm=1" {
			isBestmatchA = true
		}
		idsA := strings.SplitN(pA[2], ",", -1)

		pB := strings.SplitN(lineB.Text, "|", 3)
		queryNumberB := pB[0]
		isBestmatchB := false
		if pB[1] == "bm=1" {
			isBestmatchB = true
		}
		idsB := strings.SplitN(pB[2], ",", -1)

		if queryNumberA != queryNumberB {
			log.Panicf(
				"at query #%s in file %s but query #%s in file %s on the same line",
				queryNumberA, lineA.FromFile,
				queryNumberB, lineB.FromFile,
			)
		}

		if isBestmatchA != isBestmatchB {
			log.Panicf(
				"query #%s in file %s as %s but query #%s in file %s as %s on the same line",
				queryNumberA, lineA.FromFile, pA[1],
				queryNumberB, lineB.FromFile, pB[1],
			)
		}

		if !Equal(idsA, idsB) {
			if isBestmatchA {
				stats.Bestmatch.Different++
			} else {
				stats.SortByDate.Different++
			}

			onlyA, onlyB, _ := Diff(idsA, idsB)

			if DebugPrint {
				if len(onlyA) > 0 {
					fmt.Printf(
						"#%-5s %-20s IDs found only in A : %3d\n",
						queryNumberA, lineA.FromFile, len(onlyA),
					)
				}
				if len(onlyB) > 0 {
					fmt.Printf(
						"#%-5s %-20s IDs found only in B : %3d \n",
						queryNumberA, lineB.FromFile, len(onlyB),
					)
				}
			}

			if len(onlyA) > 0 && len(onlyB) == len(onlyA) && len(idsA) == len(idsB) {
				diff := float64(len(onlyA)) / float64(len(idsA))
				if isBestmatchA {
					stats.Bestmatch.PrimaryKeyDiffRatio += diff
					stats.Bestmatch.PrimaryKeyDiffRatioCount++
				} else {
					stats.SortByDate.PrimaryKeyDiffRatio += diff
					stats.SortByDate.PrimaryKeyDiffRatioCount++
				}
			}
		} else {
			if isBestmatchA {
				stats.Bestmatch.Identical++
			} else {
				stats.SortByDate.Identical++
			}
		}

		if isBestmatchA {
			stats.Bestmatch.Total++
		} else {
			stats.SortByDate.Total++
		}

		return nil
	})

	if stats.Bestmatch.PrimaryKeyDiffRatioCount > 0 {
		stats.Bestmatch.PrimaryKeyAvgDiffPct = (stats.Bestmatch.PrimaryKeyDiffRatio / stats.Bestmatch.PrimaryKeyDiffRatioCount) * 100
	}
	if stats.SortByDate.PrimaryKeyDiffRatioCount > 0 {
		stats.SortByDate.PrimaryKeyAvgDiffPct = (stats.SortByDate.PrimaryKeyDiffRatio / stats.SortByDate.PrimaryKeyDiffRatioCount) * 100
	}
	if stats.Bestmatch.Total > 0 {
		stats.Bestmatch.DiffPct = (float64(stats.Bestmatch.Different) / float64(stats.Bestmatch.Total)) * 100
	}
	if stats.SortByDate.Total > 0 {
		stats.SortByDate.DiffPct = (float64(stats.SortByDate.Different) / float64(stats.SortByDate.Total)) * 100
	}

	fmt.Printf("Comparison:\n%s\n\n", data.ToPrettyJSON(stats))
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
