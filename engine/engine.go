package engine

import (
	"strings"
	"sync"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	log "github.com/sirupsen/logrus"
)

var Engine *engine = nil
var CountRPCRequests = 0

type engine struct {
	*gorunner.Engine
	client     *pcommon.RPCClient
	activeSets map[string]pcommon.SetJSON
	mu         sync.RWMutex
}

func (e *engine) Init() {
	if Engine == nil {
		url := "ws://localhost:" + pcommon.Env.PARSER_SERVER_PORT + "/"
		client := pcommon.RPC.NewClient(url, time.Second*2, true)
		client.Connect()
		options := gorunner.NewEngineOptions().
			SetName("Downloader").
			SetMaxSimultaneousRunner(1).SetMaxRetry(MAX_RETRY_PER_DOWLOAD_FAILED).
			SetshouldRunAgain(func(taskID string, lastExecutionTime time.Time) bool {
				return false
			})
		Engine = &engine{
			Engine:     gorunner.NewEngine(options),
			client:     client,
			activeSets: make(map[string]pcommon.SetJSON),
			mu:         sync.RWMutex{},
		}
	}
}

func (e *engine) GetSets() map[string]pcommon.SetJSON {
	mapCopy := make(map[string]pcommon.SetJSON)
	e.mu.RLock()
	for k, v := range e.activeSets {
		mapCopy[k] = v
	}
	e.mu.RUnlock()
	return mapCopy
}

func (e *engine) RefreshSets() {
	CountRPCRequests++
	setList, err := pcommon.RPC.ParserRequests.FetchAvailablePairSetList(e.client)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("Error fetching available pair set list")
		return
	}

	for id := range e.GetSets() {
		found := false
		for _, newSet := range setList {
			if newSet.Pair.BuildSetID() == id {
				found = true
				break
			}
		}
		if !found {
			e.StopSetRunners(id)
		}
	}

	newSets := make(map[string]pcommon.SetJSON)
	for _, set := range setList {
		newSets[set.Pair.BuildSetID()] = set
	}

	e.mu.Lock()
	e.activeSets = newSets
	for _, set := range e.activeSets {
		min := set.Pair.MinHistoricalDay
		max := pcommon.Format.BuildDateStr(pcommon.Env.MAX_DAYS_BACKWARD_FOR_CONSISTENCY)
		for strings.Compare(min, max) <= 0 {
			e.AddDownload(set, min)
			t, _ := pcommon.Format.StrDateToDate(min)
			min = pcommon.Format.FormatDateStr(t.Add(time.Hour * 24))
		}
	}
	e.mu.Unlock()
}

func (e *engine) AddDownload(set pcommon.SetJSON, date string) {
	if strings.Compare(date, set.Pair.MinHistoricalDay) < 0 {
		return
	}

	e.Add(buildArchiveDownloader(set.Pair.BuildSetID(), date))
}

func (e *engine) StopSetRunners(setID string) {
	e.CancelRunnersByArgs(map[string]interface{}{
		ARG_VALUE_SET_ID: setID,
	})
}
