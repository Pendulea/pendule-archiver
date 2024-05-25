package engine

import (
	"context"
	"fmt"
	"io"
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

	STAT_LAST_UPDATE = "last_update"
)

func buildURL(set pcommon.SetJSON, date string) string {
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

func autoCancelRequestDetection(runner *gorunner.Runner, abort func()) {
	//10kb per second
	maxWait := time.Duration(runner.Size().Max()/(MIN_DOWNLOAD_BYTES_PER_SECOND)) * time.Second
	time.Sleep(maxWait)
	if !runner.IsDone() {
		abort()
	}
}

func printStatus(runner *gorunner.Runner) {

	if runner.CountSteps() == 1 {
		log.WithFields(log.Fields{
			"rid":      runner.ID,
			"eta":      pcommon.Format.AccurateHumanize(runner.ETA()),
			"speed":    fmt.Sprintf("%s/s", pcommon.Format.LargeBytesToShortString(int64(runner.SizePerMillisecond()*1000))),
			"download": fmt.Sprintf("%s/%s", pcommon.Format.LargeBytesToShortString(runner.Size().Current()), pcommon.Format.LargeBytesToShortString(runner.Size().Max())),
		}).Info("Downloading...")
	} else if runner.CountSteps() == 2 {
		log.WithFields(log.Fields{
			"rid":  runner.ID,
			"size": pcommon.Format.LargeBytesToShortString(runner.Size().Max()),
			"in":   pcommon.Format.AccurateHumanize(runner.Timer()),
		}).Info("Successfully downloaded...")
	}
}

func logInterval(runner *gorunner.Runner) {
	time.Sleep(1500 * time.Millisecond)
	for !runner.IsDone() {
		printStatus(runner)
		time.Sleep(5 * time.Second)
	}
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

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return err
		}

		runner.SetStatValue(STAT_LAST_UPDATE, time.Now().UnixMilli())
		runner.AddStep()
		resp, err := client.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			Engine.Pause(TIMEBREAK_AFTER_TOO_MANY_REQUESTS)
			return fmt.Errorf("too many requests")
		}
		if resp.StatusCode == http.StatusNotFound {
			runner.DisableRetry()
			fmt.Println(req.URL)
			return fmt.Errorf("file not found")
		}
		if resp.StatusCode != http.StatusOK {
			Engine.Pause(TIMEBREAK_UNKNOWN_REQUEST_ERROR)
			return fmt.Errorf("failed to download file status: %s", resp.Status)
		}

		runner.SetSize().Max(int64(resp.ContentLength))
		go autoCancelRequestDetection(runner, abort)
		go logInterval(runner)

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
			if runner.MustInterrupt() {
				abort()
				return nil
			}

			if n > 0 {
				written, writeErr := outFile.Write(buf[:n])
				runner.SetSize().Current(runner.Size().Current() + int64(written))
				runner.SetStatValue(STAT_LAST_UPDATE, time.Now().UnixMilli())
				if writeErr != nil {
					abort()
					return writeErr
				}
			}
			if readErr == io.EOF {
				break // End of file reached
			}
			if n < 100 {
				fmt.Println(n)
			}

			if readErr != nil {
				abort()
				return readErr
			}
		}

		runner.AddStep()
		printStatus(runner)
		return nil
	})
}

func buildArchiveDownloader(setID string, date string) *gorunner.Runner {
	runner := gorunner.NewRunner("dl-" + setID + "-" + date)

	runner.Task.AddArgs(ARG_VALUE_DATE, date)
	runner.Task.AddArgs(ARG_VALUE_SET_ID, setID)

	addArchiveDownloaderProcess(runner)
	return runner
}
