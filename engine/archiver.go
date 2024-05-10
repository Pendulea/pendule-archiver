package engine

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

const (
	ARG_VALUE_DATE   = "date"
	ARG_VALUE_SET_ID = "set_id"

	STAT_LAST_UPDATE     = "last_update"
	STAT_TOTAL_SIZE      = "total_size"
	STAT_SIZE_DOWNLOADED = "size_downloaded"
)

func buildURL(set *pcommon.SetJSON, date string) string {
	if set.Pair.IsBinanceValid() {
		symbol := set.Pair.BuildBinanceSymbol()
		filename := fmt.Sprintf("%s-trades-%s.zip", symbol, date)
		if set.Pair.Futures {
			return fmt.Sprintf("https://data.binance.vision/data/futures/um/daily/trades/%s/%s", symbol, filename)
		}
		return fmt.Sprintf("https://data.binance.vision/data/spot/daily/trades/%s/%s", symbol, filename)
	}
	return ""
}

func addArchiveDownloaderProcess(runner *gorunner.Runner, set *pcommon.SetJSON) {

	runner.AddProcess(func() error {

		date, _ := gorunner.GetArg[string](runner.Args, ARG_VALUE_DATE)
		url := buildURL(set, date)
		outputFilePath := set.Pair.BuildArchivesFilePath(date, "zip")

		if _, err := os.Stat(outputFilePath); err == nil {
			return nil
		}

		client := &http.Client{}
		ctx, cancel := context.WithCancel(context.Background())
		abort := func() {
			cancel()
			os.Remove(outputFilePath)
		}

		runner.SetStatValue(STAT_LAST_UPDATE, time.Now().UnixMilli())

		go func() {
			for !runner.IsDone() {
				time.Sleep(4 * time.Second)

				startedAt := runner.StartedAt()
				totalSize := runner.StatValue(STAT_TOTAL_SIZE)
				sizeDownloaded := runner.StatValue(STAT_SIZE_DOWNLOADED)
				percent := math.Min(float64(sizeDownloaded)/float64(totalSize)*100, 100)

				eta := time.Duration((100 / percent) * float64(time.Since(startedAt)))

				speed := float64(sizeDownloaded) / time.Since(startedAt).Seconds()

				log.WithFields(log.Fields{
					"rid":      runner.ID,
					"eta":      pcommon.Format.AccurateHumanize(eta),
					"speed":    fmt.Sprintf("%s/s", pcommon.Format.LargeBytesToShortString(int64(speed))),
					"download": fmt.Sprintf("%s/%s", pcommon.Format.LargeBytesToShortString(sizeDownloaded), pcommon.Format.LargeBytesToShortString(totalSize)),
				}).Info("Downloading...")
			}
		}()

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return err
		}

		// Execute the HTTP request
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			Engine.Pause(time.Minute * 2)
		}
		if resp.StatusCode == http.StatusNotFound {
			runner.DisableRetry()
			log.WithFields(log.Fields{
				"rid": runner.ID,
				"url": url,
			}).Error("File not found")
		}

		if resp.StatusCode != http.StatusOK {
			Engine.Pause(time.Second * 30)
			return fmt.Errorf("failed to download file status: %s", resp.Status)
		}

		set.Pair.BuildArchivesFilePath(date, "zip")

		outFile, err := os.Create(outputFilePath)
		if err != nil {
			return err
		}
		defer outFile.Close()

		// Create a buffer to write the download in chunks
		buf := make([]byte, 1024*32) // 32KB buffer
		runner.SetStatValue(STAT_TOTAL_SIZE, int64(resp.ContentLength))

		for {
			n, readErr := resp.Body.Read(buf)
			if runner.MustInterrupt() {
				abort()
				return nil
			}

			if n > 0 {
				written, writeErr := outFile.Write(buf[:n])
				runner.IncrementStatValue(STAT_SIZE_DOWNLOADED, int64(written))
				runner.SetStatValue(STAT_LAST_UPDATE, time.Now().UnixMilli())
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

		log.WithFields(log.Fields{
			"rid":  runner.ID,
			"size": pcommon.Format.LargeBytesToShortString(runner.StatValue(STAT_SIZE_DOWNLOADED)),
			"in":   pcommon.Format.AccurateHumanize(time.Since(runner.StartedAt())),
		}).Info("Successfully downloaded...")

		return nil
	})
}

func buildArchiveDownloader(set *pcommon.SetJSON, date string) *gorunner.Runner {
	runner := gorunner.NewRunner("dl-" + set.Pair.BuildSetID() + "-" + date)

	runner.Task.AddArgs(ARG_VALUE_DATE, date)
	runner.Task.AddArgs(ARG_VALUE_SET_ID, set.Pair.BuildSetID())

	addArchiveDownloaderProcess(runner, set)
	return runner
}
