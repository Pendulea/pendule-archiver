package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pendulea/pendule-archiver/engine"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

func initLogger() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.TextFormatter{
		ForceColors:      true,                  // Force colored log output even if stdout is not a tty.
		FullTimestamp:    true,                  // Enable logging the full timestamp instead of just the time passed since application started.
		TimestampFormat:  "2006-01-02 15:04:05", // Set the format for the timestamp.
		DisableTimestamp: false,                 // Do not disable printing timestamps.
	})

	// Output to stdout instead of the default stderr, could also be a file.
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)
}

func cleanup() {
	engine.Engine.Quit()
}

func main() {
	initLogger()
	pcommon.Env.Init()
	engine.Engine.Init()

	go func() {
		for {
			time.Sleep(time.Second * 5)
			if engine.Engine.CountQueued() > 0 {
				fmt.Println("")
				engine.Engine.PrintStatus()
				fmt.Println("")
			}
		}
	}()

	go func() {
		for {
			engine.Engine.RefreshSets()
			time.Sleep(time.Minute)
		}
	}()

	sigs := make(chan os.Signal, 1)
	// Create a channel to communicate that the signal has been handled
	done := make(chan bool, 1)

	// Register the channel to receive notifications for specific signals
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine that will handle the signals
	go func() {
		<-sigs // Block until a signal is received
		cleanup()
		done <- true // Signal that handling is complete
	}()

	<-done // Block until the signal has been handled
	log.Info("Exiting...")

}
