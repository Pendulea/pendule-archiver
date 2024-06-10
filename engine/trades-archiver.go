package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

const TRADE_TYPE = "trades"
const BOOK_DEPTH_TYPE = "book_depth"
const METRICS_TYPE = "metrics"

const (
	ARG_VALUE_DATE   = "date"
	ARG_VALUE_SET_ID = "set_id"
)

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
		return fmt.Errorf("too many requests")
	}
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("file not found")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download file status: %s", resp.Status)
	}

	fileSize := resp.ContentLength
	if fileSize <= 0 {
		return errors.New("invalid file size")
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
			return errors.New("interrupted")
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
		setID, _ := gorunner.GetArg[string](runner.Args, ARG_VALUE_SET_ID)

		set, ok := Engine.GetSets()[setID]
		if !ok {
			fmt.Println("set not found")
			return nil
		}

		lastLogs := make(map[string]int64)
		pathToRM := []string{}

		printProgressLog := func(title string, current int64, total int64, startedAt time.Time) {
			if current == total {
				log.WithFields(log.Fields{
					"rid":  runner.ID,
					"size": pcommon.Format.LargeBytesToShortString(total),
					"in":   pcommon.Format.AccurateHumanize(time.Since(startedAt)),
				}).Infof("Successfully downloaded %s...", title)
				return
			}

			if time.Since(startedAt).Seconds() < 1.5 {
				return
			}

			if last, ok := lastLogs[title]; ok {
				if time.Since(time.Unix(0, last)).Seconds() < 3 {
					return
				}
			}

			lastLogs[title] = time.Now().UnixNano()
			percent := float64(current) / float64(total) * 100
			eta := time.Since(startedAt).Seconds() / percent * (100 - percent)
			speedPerSec := int64(float64(current) / time.Since(startedAt).Seconds())

			log.WithFields(log.Fields{
				"rid":      runner.ID,
				"eta":      pcommon.Format.AccurateHumanize(time.Duration(int64(eta) * int64(time.Second))),
				"speed":    fmt.Sprintf("%s/s", pcommon.Format.LargeBytesToShortString(speedPerSec)),
				"download": fmt.Sprintf("%s/%s", pcommon.Format.LargeBytesToShortString(current), pcommon.Format.LargeBytesToShortString(total)),
			}).Infof("Downloading %s...", title)
		}

		handleDownloadError := func(perfectURL string, title string, err error) error {

			removePendengFiles := func() {
				for _, path := range pathToRM {
					fmt.Print("Removing: ", path, "\n")
					os.Remove(path)
				}
			}

			checkRouteIsValid := func() bool {
				resp, err := http.Head(perfectURL) // Perform a HEAD request
				if err != nil {
					return false
				}
				resp.Body.Close() // Ensure we close the response body
				return resp.StatusCode == 200
			}

			if err != nil {
				if strings.Contains(err.Error(), "too many requests") {
					Engine.Pause(TIMEBREAK_AFTER_TOO_MANY_REQUESTS)
				}
				if strings.Contains(err.Error(), "file not found") {
					if checkRouteIsValid() {
						var csvPath string
						var zipPath string

						if title == TRADE_TYPE {
							csvPath = set.Pair.BuildTradesArchivesFilePath(date, "csv")
							zipPath = set.Pair.BuildTradesArchivesFilePath(date, "zip")
						} else if title == BOOK_DEPTH_TYPE {
							csvPath = set.Pair.BuildBookDepthArchivesFilePath(date, "csv")
							zipPath = set.Pair.BuildBookDepthArchivesFilePath(date, "zip")
						} else if title == METRICS_TYPE {
							csvPath = set.Pair.BuildFuturesMetricsArchivesFilePath(date, "csv")
							zipPath = set.Pair.BuildFuturesMetricsArchivesFilePath(date, "zip")
						} else {
							log.Fatal("Unknown title")
						}

						f, err := os.Create(csvPath)
						if err != nil {
							removePendengFiles()
							return err
						}
						f.Close()
						if err := pcommon.File.ZipFile(csvPath, zipPath); err != nil {
							removePendengFiles()
							return err
						}
						os.Remove(csvPath)
						fmt.Printf("File not found for %s (%s), but empty zip archive created as replacement.\n", title, date)
						return nil
					}
					runner.DisableRetry()
				}
				if strings.Contains(err.Error(), "failed to download file") || strings.Contains(err.Error(), "interrupted") {
					Engine.Pause(TIMEBREAK_UNKNOWN_REQUEST_ERROR)
				}
				removePendengFiles()
			}
			return err
		}

		doTheTask := func(minDay string, url string, perfectURL string, outputFP string, dataType string) error {
			if minDay != "" && strings.Compare(date, minDay) >= 0 {
				if _, err := os.Stat(outputFP); err == nil {
					return nil
				}
				pathToRM = append(pathToRM, outputFP)
				startedAt := time.Now()
				return handleDownloadError(perfectURL, dataType, downloadFile(url, outputFP, runner.MustInterrupt, func(current int64, total int64) {
					printProgressLog(dataType, current, total, startedAt)
				}))
			}
			return nil
		}

		if set.Pair.HasFutures {
			if err := doTheTask(set.Pair.MinBookDepthHistoricalDay,
				set.Pair.BuildBinanceBookDepthArchiveURL(date),
				set.Pair.BuildBinanceBookDepthArchiveURL(set.Pair.MinBookDepthHistoricalDay),
				set.Pair.BuildBookDepthArchivesFilePath(date, "zip"),
				BOOK_DEPTH_TYPE); err != nil {
				return err
			}

			if err := doTheTask(set.Pair.MinFuturesMetricsHistoricalDay,
				set.Pair.BuildBinanceFuturesMetricsArchiveURL(date),
				set.Pair.BuildBinanceFuturesMetricsArchiveURL(set.Pair.MinFuturesMetricsHistoricalDay),
				set.Pair.BuildFuturesMetricsArchivesFilePath(date, "zip"),
				METRICS_TYPE); err != nil {
				return err
			}

		}

		return doTheTask(set.Pair.MinHistoricalDay,
			set.Pair.BuildBinanceTradesArchiveURL(date),
			set.Pair.BuildBinanceTradesArchiveURL(set.Pair.MinHistoricalDay),
			set.Pair.BuildTradesArchivesFilePath(date, "zip"),
			TRADE_TYPE)
	})

}

func buildArchiveDownloader(setID string, date string) *gorunner.Runner {
	runner := gorunner.NewRunner("dl-" + setID + "-" + date)

	runner.Task.AddArgs(ARG_VALUE_DATE, date)
	runner.Task.AddArgs(ARG_VALUE_SET_ID, setID)

	addArchiveDownloaderProcess(runner)
	return runner
}
