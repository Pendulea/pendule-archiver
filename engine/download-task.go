package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

const (
	ARG_VALUE_DATE         = "date"
	ARG_VALUE_SET          = "set"
	ARG_VALUE_ARCHIVE_TYPE = "archive_type"
)

const MIN_DOWNLOAD_BYTES_PER_SECOND = 10 * 1024
const TIMEBREAK_AFTER_TOO_MANY_REQUESTS = 2 * time.Minute
const TIMEBREAK_UNKNOWN_REQUEST_ERROR = 30 * time.Second

const FILE_NOT_FOUND_ERROR = "file not found"
const TOO_MANY_REQUESTS_ERROR = "too many requests"
const FAILED_DOWNLOAD_ERROR = "failed to download file"
const INVALID_FILE_SIZE_ERROR = "invalid file size"
const INTERRUPTED_ERROR = "interrupted"

func downloadFile(url string, outputFilePath string, interruptionCheck func() bool, statusChange func(current int64, total int64)) error {
	if _, err := os.Stat(outputFilePath); err == nil {
		return nil
	}

	client := &http.Client{}
	ctx, cancel := context.WithCancel(context.Background())
	abort := func() {
		fmt.Printf("Aborting download of %s\n", url)
		cancel()
		os.Remove(outputFilePath)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf(TOO_MANY_REQUESTS_ERROR)
	}
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf(FILE_NOT_FOUND_ERROR)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(FAILED_DOWNLOAD_ERROR+" status: %s", resp.Status)
	}

	fileSize := resp.ContentLength
	if fileSize <= 0 {
		return errors.New(INVALID_FILE_SIZE_ERROR)
	}

	var currentSize int64 = 0
	statusChange(0, fileSize)

	timedout := new(bool)
	*timedout = false

	//anto cancel request detection after 10kb per second
	go func(fileSize int64, timedout *bool) {
		maxWait := time.Duration(fileSize/(MIN_DOWNLOAD_BYTES_PER_SECOND))*time.Second + time.Second*3
		time.Sleep(maxWait)
		*timedout = true
	}(fileSize, timedout)

	defer resp.Body.Close()

	outFile, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Create a buffer to write the download in chunks
	buf := make([]byte, 1024*32) // 32KB buffer

	for {
		n, readErr := resp.Body.Read(buf)
		if interruptionCheck() || *timedout {
			abort()
			return errors.New(INTERRUPTED_ERROR)
		}

		if n > 0 {
			written, writeErr := outFile.Write(buf[:n])
			currentSize += int64(written)
			statusChange(currentSize, fileSize)
			if writeErr != nil {
				abort()
				return writeErr
			}
		}
		if readErr == io.EOF {
			break // End of file reached
		}
		if readErr != nil {
			abort()
			return readErr
		}
	}
	return nil
}

func addArchiveDownloaderProcess(runner *gorunner.Runner) {
	runner.AddProcess(func() error {
		date, _ := gorunner.GetArg[string](runner.Args, ARG_VALUE_DATE)
		set, _ := gorunner.GetArg[*pcommon.SetJSON](runner.Args, ARG_VALUE_SET)
		t, _ := gorunner.GetArg[pcommon.ArchiveType](runner.Args, ARG_VALUE_ARCHIVE_TYPE)

		outputFP := t.GetArchiveZipPath(date, set)

		//check if file already exist
		if _, err := os.Stat(outputFP); err == nil {
			return nil
		}

		//check if all fragmented archives are built
		list := t.GetTargetedAssets()
		foundCount := 0
		for _, asset := range list {
			archiveZipPath := set.Settings.BuildArchiveFilePath(asset, date, "zip")
			if _, err := os.Stat(archiveZipPath); err == nil {
				foundCount++
			}
		}
		if foundCount == len(list) {
			return nil
		}

		if err := pcommon.File.EnsureDir(filepath.Dir(outputFP)); err != nil {
			return err
		}

		if t == pcommon.BINANCE_SPOT_TRADES {
			symbol := strings.ToUpper(set.Settings.ID[0] + set.Settings.ID[1])
			filename := fmt.Sprintf("%s-trades-%s.zip", symbol, date)
			path := filepath.Join(os.Getenv("ARCHIVES_DIR"), symbol, "_spot", filename)

			if _, err := os.Stat(path); err == nil {
				if err := os.Rename(path, outputFP); err != nil {
					return err
				}
				os.Remove(path)
				return nil
			}
		}

		if t == pcommon.BINANCE_FUTURES_TRADES {
			symbol := strings.ToUpper(set.Settings.ID[0] + set.Settings.ID[1])
			filename := fmt.Sprintf("%s-trades-%s.zip", symbol, date)
			path := filepath.Join(os.Getenv("ARCHIVES_DIR"), symbol, "_futures", filename)
			if _, err := os.Stat(path); err == nil {
				if err := os.Rename(path, outputFP); err != nil {
					return err
				}
				os.Remove(path)
				return nil
			}
		}

		if t == pcommon.BINANCE_BOOK_DEPTH {
			symbol := strings.ToUpper(set.Settings.ID[0] + set.Settings.ID[1])
			filename := fmt.Sprintf("%s-bookDepth-%s.zip", symbol, date)
			path := filepath.Join(os.Getenv("ARCHIVES_DIR"), symbol, "book_depth", filename)
			if _, err := os.Stat(path); err == nil {
				if err := os.Rename(path, outputFP); err != nil {
					return err
				}
				os.Remove(path)
				return nil
			}
		}
		if t == pcommon.BINANCE_METRICS {
			symbol := strings.ToUpper(set.Settings.ID[0] + set.Settings.ID[1])
			filename := fmt.Sprintf("%s-metrics-%s.zip", symbol, date)
			path := filepath.Join(os.Getenv("ARCHIVES_DIR"), symbol, "metrics", filename)
			if _, err := os.Stat(path); err == nil {
				if err := os.Rename(path, outputFP); err != nil {
					return err
				}
				os.Remove(path)
				return nil
			}
		}

		url, err := t.GetURL(date, set)
		if err != nil {
			return err
		}

		lastLogs := make(map[pcommon.ArchiveType]int64)
		printProgressLog := func(t pcommon.ArchiveType, current int64, total int64, startedAt time.Time) {
			if current == total {
				log.WithFields(log.Fields{
					"rid":  runner.ID,
					"size": pcommon.Format.LargeBytesToShortString(total),
					"in":   pcommon.Format.AccurateHumanize(time.Since(startedAt)),
				}).Infof("Successfully downloaded %s...", t)
				return
			}

			if time.Since(startedAt).Seconds() < 1.5 {
				return
			}

			if last, ok := lastLogs[t]; ok {
				if time.Since(time.Unix(0, last)).Seconds() < 3 {
					return
				}
			}

			lastLogs[t] = time.Now().UnixNano()
			percent := float64(current) / float64(total) * 100
			eta := time.Since(startedAt).Seconds() / percent * (100 - percent)
			speedPerSec := int64(float64(current) / time.Since(startedAt).Seconds())

			log.WithFields(log.Fields{
				"rid":      runner.ID,
				"eta":      pcommon.Format.AccurateHumanize(time.Duration(int64(eta) * int64(time.Second))),
				"speed":    fmt.Sprintf("%s/s", pcommon.Format.LargeBytesToShortString(speedPerSec)),
				"download": fmt.Sprintf("%s/%s", pcommon.Format.LargeBytesToShortString(current), pcommon.Format.LargeBytesToShortString(total)),
			}).Infof("Downloading %s...", t)
		}

		handleDownloadError := func(perfectURL string, t pcommon.ArchiveType, err error) error {

			checkRouteIsValid := func() bool {
				resp, err := http.Head(perfectURL) // Perform a HEAD request
				if err != nil {
					return false
				}
				resp.Body.Close() // Ensure we close the response body
				return resp.StatusCode == 200
			}

			if err != nil {
				archiveIndex, found := pcommon.ArchivesIndex[t]
				if !found {
					log.Warn("Archive index not found")
					return err
				}

				if strings.Contains(err.Error(), TOO_MANY_REQUESTS_ERROR) {
					Engine.Pause(TIMEBREAK_AFTER_TOO_MANY_REQUESTS)
				}
				if strings.Contains(err.Error(), FILE_NOT_FOUND_ERROR) {

					xxDaysAgo := pcommon.Format.BuildDateStr(archiveIndex.ConsistencyMaxLookbackDays)
					if strings.Compare(xxDaysAgo, date) <= 0 {
						runner.DisableRetry()
						return err
					} else if checkRouteIsValid() {

						ext := filepath.Ext(outputFP)
						fp := outputFP
						if ext != ".csv" {
							fp = strings.Replace(outputFP, ext, ".csv", 1)
						}
						f, err := os.Create(fp)
						if err != nil {
							return err
						}
						f.Close()
						if ext == ".zip" {
							if err := pcommon.File.ZipFile(fp, outputFP); err != nil {
								return err
							}
							os.Remove(fp)
						}

						fmt.Printf("File not found for %s (%s), but empty zip archive created as replacement.\n", t, date)
						return nil
					}
					runner.DisableRetry()
				}
				if strings.Contains(err.Error(), FAILED_DOWNLOAD_ERROR) || strings.Contains(err.Error(), INTERRUPTED_ERROR) {
					Engine.Pause(TIMEBREAK_UNKNOWN_REQUEST_ERROR)
				}
			}
			return err
		}

		startedAt := time.Now()
		err = downloadFile(url, outputFP, runner.MustInterrupt, func(current int64, total int64) {
			printProgressLog(t, current, total, startedAt)
		})

		if err != nil {
			date := ""
			for _, a := range t.GetTargetedAssets() {
				for _, sass := range set.Assets {
					if a == sass.Address.AssetType {
						c := sass.FindConsistencyByTimeframe(time.Duration(Engine.status.MinTimeframe) * time.Millisecond)
						if c == nil {
							return err
						}
						date = pcommon.Format.FormatDateStr(c.Range[0].ToTime())
						break
					}
				}
			}
			perfectURL, err := t.GetURL(date, set)
			if err != nil {
				log.Warn("Failed to get perfect URL")
				return err
			}
			if e := handleDownloadError(perfectURL, t, err); e != nil {
				return e
			}
		}

		return nil
	})

}

func buildArchiveDownloader(date string, set *pcommon.SetJSON, t pcommon.ArchiveType) *gorunner.Runner {

	id := fmt.Sprintf("dl-%s-%s-%s", set.Settings.IDString(), date, string(t))
	runner := gorunner.NewRunner(id)

	runner.AddArgs(ARG_VALUE_DATE, date)
	runner.AddArgs(ARG_VALUE_SET, set)
	runner.AddArgs(ARG_VALUE_ARCHIVE_TYPE, t)

	addArchiveDownloaderProcess(runner)

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
