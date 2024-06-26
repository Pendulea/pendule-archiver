package engine

import (
	"strings"
	"sync"
	"time"

	"github.com/fantasim/gorunner"
	pcommon "github.com/pendulea/pendule-common"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
)

var Engine *engine = nil
var CountRPCRequests = 0

type engine struct {
	*gorunner.Engine
	client     *pcommon.RPCClient
	activeSets map[string]pcommon.SetJSON
	status     *pcommon.GetStatusResponse
	mu         sync.RWMutex
}

func (e *engine) Init() {
	if Engine == nil {
		url := "ws://localhost:" + pcommon.Env.PARSER_SERVER_PORT + "/"
		client := pcommon.RPC.NewClient(url, time.Second*2, true)
		client.Connect()
		options := gorunner.NewEngineOptions().
			SetName("Archiver").
			SetMaxSimultaneousRunner(4).SetMaxRetry(MAX_RETRY_PER_DOWLOAD_FAILED).
			SetshouldRunAgain(func(taskID string, lastExecutionTime time.Time) bool {
				return time.Since(lastExecutionTime) > time.Hour*6
			})
		Engine = &engine{
			Engine:     gorunner.NewEngine(options),
			client:     client,
			activeSets: make(map[string]pcommon.SetJSON),
			mu:         sync.RWMutex{},
		}
	}
}

func (e *engine) refreshStatus() error {
	CountRPCRequests++
	status, err := pcommon.RPC.ParserRequests.FetchStatus(e.client)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("Error fetching status")
		return err
	}
	e.status = status
	return nil
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
	if e.status == nil {
		err := e.refreshStatus()
		if err != nil {
			return
		}
	}

	CountRPCRequests++
	setList, err := pcommon.RPC.ParserRequests.FetchAvailableSetList(e.client)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("Error fetching available pair set list")
		return
	}

	for id := range e.GetSets() {
		found := false

		for _, newSet := range setList {
			if newSet.Settings.IDString() == id {
				found = true
				break
			}
		}

		if !found {
			// e.StopSetRunners(id)
		}
	}

	newSets := make(map[string]pcommon.SetJSON)
	for _, set := range setList {
		newSets[set.Settings.IDString()] = set
	}

	e.mu.Lock()
	e.activeSets = newSets
	for _, set := range e.activeSets {
		handleSet(e, &set)
	}
	e.mu.Unlock()
}

func handleSet(e *engine, set *pcommon.SetJSON) error {
	_, err := GetSetType(set.Settings)
	if err != nil {
		return err
	}

	type DL struct {
		AssetID pcommon.AssetType
		Date    string
	}

	list := []DL{}
	for _, asset := range set.Assets {
		if asset.Timeframe == e.status.MinTimeframe {
			assetMax := asset.ConsistencyRange[1]
			max := pcommon.Format.BuildDateStr(asset.ConsistencyMaxLookbackDays)
			for t := assetMax; strings.Compare(pcommon.Format.FormatDateStr(t.ToTime()), max) == -1; t = t.Add(time.Hour * 24) {
				list = append(list, DL{
					AssetID: asset.ID,
					Date:    pcommon.Format.FormatDateStr(t.ToTime()),
				})
			}
		}
	}
	filtered := lo.UniqBy(lo.Map(list, func(i DL, index int) []string {
		t := GetRequiredArchiveType(i.AssetID)
		return []string{string(t), i.Date}
	}), func(i []string) string {
		return i[0] + i[1]
	})

	for _, v := range filtered {
		// e.Add(buildArchiveDownloader(v[1], set, ArchiveType(v[0])))
		e.Add(buildArchiveFragmenter(v[1], set, ArchiveType(v[0])))
		return nil
	}

	return nil
}

// func (e *engine) StopSetRunners(setID string) {
// 	e.CancelRunnersByArgs(map[string]interface{}{
// 		ARG_VALUE_SET_ID: setID,
// 	})
// }
