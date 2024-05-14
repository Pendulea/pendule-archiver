package engine

import (
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

var Engine *engine = nil
var CountRPCRequests = 0

type engine struct {
	*gorunner.Engine
	client *pcommon.RPCClient
}

func (e *engine) Init() {
	if Engine == nil {

		url := "ws://localhost:" + pcommon.Env.PARSER_SERVER_PORT + "/"
		client := pcommon.RPC.NewClient(url, time.Second*2, true)
		client.Connect()
		options := gorunner.NewEngineOptions().
			SetName("Downloader").
			SetMaxSimultaneousRunner(SIMULTANEOUS_DOWNLOADS).SetMaxRetry(MAX_RETRY_PER_DOWLOAD_FAILED).
			SetshouldRunAgain(func(taskID string, lastExecutionTime time.Time) bool {
				return false
			})
		Engine = &engine{
			Engine: gorunner.NewEngine(options),
			client: client,
		}
	}
}

func (e *engine) RefreshSets(currentSets *WorkingSets) {
	CountRPCRequests++
	newSets, err := pcommon.RPC.ParserRequests.FetchAvailablePairSetList(e.client)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("Error fetching available pair set list")
		return
	}

	for id := range *currentSets {
		found := false
		for _, newSet := range newSets {
			if newSet.Pair.BuildSetID() == id {
				found = true
				break
			}
		}
		if !found {
			currentSets.Remove(id)
			e.StopSetRunners(id)
		}
	}

	for _, newSet := range newSets {
		if s := currentSets.Add(&newSet); s != nil {
			for _, date := range s.Inconsistencies {
				e.AddDownload(currentSets, s.Pair.BuildSetID(), date)
			}
		}
	}
}

func (e *engine) AddDownload(activeSets *WorkingSets, setID string, date string) {
	set := activeSets.Find(setID)
	if set == nil {
		log.WithFields(log.Fields{
			"symbol": setID,
			"date":   date,
		}).Error("Set not found")
		return
	}
	CountRPCRequests++
	parsed, err := pcommon.RPC.ParserRequests.IsDateParsed(e.client, pcommon.IsDateParsedRequest{
		SetID:     set.Pair.BuildSetID(),
		Date:      date,
		TimeFrame: pcommon.MIN_TIME_FRAME.Milliseconds(),
	})
	if err != nil {
		log.WithFields(log.Fields{
			"symbol": set.Pair.BuildSetID(),
			"date":   date,
			"error":  err.Error(),
		}).Error("Error checking if date is parsed")
		return
	}
	if !parsed {
		e.Add(buildArchiveDownloader(set, date))
	}
}

func (e *engine) StopSetRunners(setID string) {
	args := map[string]interface{}{
		ARG_VALUE_SET_ID: setID,
	}
	e.StopRunnersByArgs(args)
}
