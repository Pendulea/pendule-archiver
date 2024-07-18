package engine

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
)

func addArchiveFragmenterProcess(runner *gorunner.Runner) {
	runner.AddProcess(func() error {
		date, _ := gorunner.GetArg[string](runner.Args, ARG_VALUE_DATE)
		set, _ := gorunner.GetArg[*pcommon.SetJSON](runner.Args, ARG_VALUE_SET)
		t, _ := gorunner.GetArg[pcommon.ArchiveType](runner.Args, ARG_VALUE_ARCHIVE_TYPE)

		archivePath := t.GetArchiveZipPath(date, set.Settings)
		stat, err := os.Stat(archivePath)
		if err != nil {
			return err
		}

		logData := struct {
			step  int
			asset pcommon.AssetType
			i     int
			total int
		}{
			step:  0,
			asset: pcommon.AssetType(""),
			i:     0,
			total: 0,
		}

		logPlease := func() {
			step, asset, i, total := logData.step, logData.asset, logData.i, logData.total

			if step == 0 {
				log.WithFields(log.Fields{
					"size": pcommon.Format.LargeBytesToShortString(stat.Size()),
				}).Info(fmt.Sprintf("Unzipping %s (%s) archive (%s)", t, date, set.Settings.IDString()))
			} else if step == 1 {
				log.WithFields(log.Fields{
					"size": pcommon.Format.LargeBytesToShortString(int64(float64(stat.Size()) * 5.133)),
				}).Info(fmt.Sprintf("Parsing %s (%s) archive (%s)", t, date, set.Settings.IDString()))
			} else if step == 2 {
				if total == i {
					log.WithFields(log.Fields{}).Info(fmt.Sprintf("Zipping %s (%s) assets (%s)", t, date, set.Settings.IDString()))
				} else {
					p := float64(i) / float64(total) * 100
					log.WithFields(log.Fields{
						"progress": fmt.Sprintf("%.2f%%", p),
					}).Info(fmt.Sprintf("Building %s (%s) asset (%s)", asset, date, set.Settings.IDString()))
				}
			} else if step == 3 {
				log.WithFields(log.Fields{}).Info(fmt.Sprintf("Successfully built %s (%s) asset (%s)", asset, date, set.Settings.IDString()))
			}
		}

		go func() {
			time.Sleep(time.Second * 2)
			for !runner.IsDone() {
				logPlease()
				time.Sleep(time.Second * 5)
			}
		}()

		archiveExt := filepath.Ext(archivePath)
		archiveDir := strings.Replace(archivePath, archiveExt, "", 1)
		if archiveExt == ".zip" {
			defer os.Remove(archiveDir + ".csv")
			defer os.RemoveAll(archiveDir)

			err := pcommon.File.UnzipFile(archivePath, archiveDir)
			if err != nil {
				if err.Error() == "zip: not a valid zip file" {
					os.Remove(archivePath)
				}
				return err
			}
			listCSVFiles, err := os.ReadDir(archiveDir)
			if err != nil {
				return err
			}
			list := lo.Filter(listCSVFiles, func(f os.DirEntry, idx int) bool {
				return filepath.Ext(f.Name()) == ".csv"
			})
			if len(list) != 1 {
				return fmt.Errorf("invalid number of csv files")
			}
			if err := os.Rename(filepath.Join(archiveDir, list[0].Name()), archiveDir+".csv"); err != nil {
				return err
			}
		} else if archiveExt != ".csv" {
			return fmt.Errorf("invalid extension")
		}

		logData.step = 1
		lines, headerXY, err := ParseFromCSV(archiveDir + ".csv")
		if err != nil {
			return err
		}
		logData.total = len(lines)

		tree := pcommon.ArchivesIndex[t]
		computedTimes := make([]string, len(lines))
		timeTitle := strings.ToLower(tree.Time.OriginColumnTitle)

		for i, line := range lines {
			done := false
			var err error = nil
			if timeTitle != "" {
				if idx, ok := headerXY[timeTitle]; ok {
					if tree.Time.DataFilter == nil {
						computedTimes[i] = line[idx]
					} else {
						computedTimes[i], err = tree.Time.DataFilter(line[idx], line, headerXY)
					}
					done = true
				}
			}
			if !done && tree.Time.OriginColumnIndex >= 0 {
				if tree.Time.DataFilter == nil {
					computedTimes[i] = line[tree.Time.OriginColumnIndex]
				} else {
					computedTimes[i], err = tree.Time.DataFilter(line[tree.Time.OriginColumnIndex], line, headerXY)
				}
				done = true
			}

			if err != nil {
				return err
			}

			if !done {
				return fmt.Errorf("can't find the column")
			}
		}

		filesToRM := []string{}
		rmAllFiles := func() {
			for _, f := range filesToRM {
				os.Remove(f)
			}
		}

		var assetJSON *pcommon.AssetJSON = nil
		for _, col := range tree.Columns {
			for _, asset := range set.Assets {
				if asset.Address.AssetType == col.Asset {
					assetJSON = &asset
					break
				}
			}
			logData.step = 2
			logData.asset = col.Asset

			csvFilePath := set.Settings.BuildArchiveFilePath(col.Asset, date, "csv")
			zipFilePath := set.Settings.BuildArchiveFilePath(col.Asset, date, "zip")

			filesToRM = append(filesToRM, csvFilePath, zipFilePath)

			if err := pcommon.File.EnsureDir(filepath.Dir(csvFilePath)); err != nil {
				rmAllFiles()
				return err
			}

			file, err := os.Create(csvFilePath)
			if err != nil && os.IsExist(err) {
				continue
			}
			if err != nil {
				rmAllFiles()
				return err
			}
			writer := csv.NewWriter(file)

			//write header
			if err := writer.Write([]string{string(pcommon.ColumnType.TIME), string(col.Asset)}); err != nil {
				rmAllFiles()
				return err
			}

			colTitle := strings.ToLower(col.OriginColumnTitle)

			for idx, line := range lines {
				logData.i = idx + 1
				value := ""
				found := false
				var err error = nil

				if colTitle != "" {
					if idx, ok := headerXY[colTitle]; ok {
						if col.DataFilter == nil {
							value = line[idx]
						} else {
							value, err = col.DataFilter(line[idx], line, headerXY)
						}
						found = true
					}
				}
				if !found && col.OriginColumnIndex >= 0 {
					if col.DataFilter == nil {
						value = line[col.OriginColumnIndex]
					} else {
						value, err = col.DataFilter(line[col.OriginColumnIndex], line, headerXY)
					}
					found = true
				}

				if err != nil {
					rmAllFiles()
					return err
				}

				if !found {
					rmAllFiles()
					return fmt.Errorf("can't find the column")
				}

				value = strings.TrimSpace(value)
				if len(value) > 0 {
					if assetJSON != nil {
						if v, err := strconv.ParseFloat(value, 64); err == nil {
							value = pcommon.Format.Float(v, assetJSON.Decimals)
						}
					}
					if err := writer.Write([]string{strings.TrimSpace(computedTimes[idx]), value}); err != nil {
						rmAllFiles()
						return err
					}
				}
				if idx%10000 == 0 {
					writer.Flush()
				}
			}

			writer.Flush()
			if err := file.Close(); err != nil {
				rmAllFiles()
				return err
			}
			if err := pcommon.File.ZipFile(csvFilePath, zipFilePath); err != nil {
				rmAllFiles()
				return err
			}

			os.Remove(csvFilePath)
			logData.step = 3
			logPlease()
		}

		return nil
	})
}

func buildArchiveFragmenter(date string, set *pcommon.SetJSON, t pcommon.ArchiveType) *gorunner.Runner {

	id := fmt.Sprintf("frag-%s-%s-%s", set.Settings.IDString(), date, string(t))
	runner := gorunner.NewRunner(id)

	runner.AddArgs(ARG_VALUE_DATE, date)
	runner.AddArgs(ARG_VALUE_SET, set)
	runner.AddArgs(ARG_VALUE_ARCHIVE_TYPE, t)

	addArchiveFragmenterProcess(runner)

	runner.AddRunningFilter(func(details gorunner.EngineDetails, runner *gorunner.Runner) bool {
		date, _ := gorunner.GetArg[string](runner.Args, ARG_VALUE_DATE)
		set, _ := gorunner.GetArg[*pcommon.SetJSON](runner.Args, ARG_VALUE_SET)
		t, _ := gorunner.GetArg[pcommon.ArchiveType](runner.Args, ARG_VALUE_ARCHIVE_TYPE)

		for _, r := range details.RunningRunners {
			date2, _ := gorunner.GetArg[string](r.Args, ARG_VALUE_DATE)
			set2, _ := gorunner.GetArg[*pcommon.SetJSON](r.Args, ARG_VALUE_SET)
			t2, _ := gorunner.GetArg[pcommon.ArchiveType](r.Args, ARG_VALUE_ARCHIVE_TYPE)

			if date == date2 && set.Settings.IDString() == set2.Settings.IDString() && t == t2 {
				return false
			}
		}
		return true
	})

	return runner
}

func ParseFromCSV(fp string) ([][]string, map[string]int, error) {
	headerCoord := map[string]int{}

	file, err := os.Open(fp)
	if err != nil {
		return nil, headerCoord, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ',' // Set the delimiter to comma
	reader.TrimLeadingSpace = true

	var lines [][]string

	// Check if the CSV is empty
	firstRow, err := reader.Read()
	if err == io.EOF {
		// CSV is empty, return an empty slice
		return lines, headerCoord, nil
	}
	if err != nil {
		return nil, headerCoord, err
	}

	// Determine if the first row is a header or a data row
	if isHeader(firstRow) {

		for idx, field := range firstRow {
			headerCoord[strings.ToLower(field)] = idx
		}

		// Read the next row if the first row is a header
		firstRow, err = reader.Read()
		if err == io.EOF {
			// CSV only contains a header, return an empty slice
			return lines, headerCoord, nil
		}
		if err != nil {
			return nil, headerCoord, err
		}
	}

	lines = append(lines, firstRow)

	for {
		fields, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, headerCoord, err
		}

		lines = append(lines, fields)
	}
	return lines, headerCoord, nil
}

// Example function to determine if a row is a header
func isHeader(row []string) bool {
	for _, field := range row {
		if strings.Contains(strings.ToLower(field), "time") || strings.Contains(strings.ToLower(field), "date") || strings.Contains(strings.ToLower(field), "id") {
			return true
		}
	}
	return false
}
