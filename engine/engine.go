package engine

import (
	"fmt"
	"os"
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
	activeSets map[string]*pcommon.SetJSON
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
			SetMaxSimultaneousRunner(2).SetMaxRetry(MAX_RETRY_PER_DOWLOAD_FAILED).
			SetshouldRunAgain(func(taskID string, lastExecutionTime time.Time) bool {
				return time.Since(lastExecutionTime) > time.Hour*6
			})
		Engine = &engine{
			Engine:     gorunner.NewEngine(options),
			client:     client,
			activeSets: make(map[string]*pcommon.SetJSON),
			mu:         sync.RWMutex{},
		}
		go func(eng *engine) {
			for {
				time.Sleep(time.Second * 5)
				if eng.CountQueued() > 0 {
					fmt.Println("")
					eng.PrintStatus()
					fmt.Println("")
				}
			}
		}(Engine)
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
	e.mu.Lock()
	mapID := make(map[string]bool)
	for _, newSet := range setList {
		id := newSet.Settings.IDString()
		if _, ok := e.activeSets[id]; !ok {
			e.activeSets[id] = &newSet
		}
		mapID[id] = true
	}
	for _, set := range e.activeSets {
		id := set.Settings.IDString()
		if _, ok := mapID[id]; !ok {
			go e.StopSetRunners(set)
			delete(e.activeSets, id)
		}
	}
	e.mu.Unlock()
	for _, set := range e.activeSets {
		handleSet(e, set)
	}
}

func handleSet(e *engine, set *pcommon.SetJSON) error {
	if err := set.Settings.IsValid(); err != nil {
		return err
	}

	type DL struct {
		AssetID pcommon.AssetType
		Date    string
	}

	list := []DL{}
	for _, asset := range set.Assets {
		c := asset.FindConsistencyByTimeframe(time.Duration(e.status.MinTimeframe) * time.Millisecond)
		if c == nil {
			continue
		}

		assetMax := c.Range[1]
		max := pcommon.Format.BuildDateStr(asset.ConsistencyMaxLookbackDays)
		for t := assetMax; strings.Compare(pcommon.Format.FormatDateStr(t.ToTime()), max) == -1; t = t.Add(time.Hour * 24) {
			list = append(list, DL{
				AssetID: asset.Address.AssetType,
				Date:    pcommon.Format.FormatDateStr(t.ToTime()),
			})
		}
	}
	filtered :=
		lo.UniqBy(
			lo.Filter(
				//map
				lo.Map(list, func(i DL, index int) []string {
					t := i.AssetID.GetRequiredArchiveType()
					if t == nil {
						return nil
					}
					return []string{string(*t), i.Date}
				}),
				//filter
				func(elem []string, index int) bool {
					return elem != nil
				}),
			//uniqBy
			func(i []string) string {
				return i[0] + i[1]
			})

	if len(filtered) > 0 {

		checked := map[pcommon.ArchiveType]bool{}
		for _, v := range filtered {
			archiveType := pcommon.ArchiveType(v[0])
			date := v[1]
			if _, ok := checked[archiveType]; !ok {
				_, err := archiveType.GetURL(date, set)
				if err != nil {
					return err
				}
				checked[archiveType] = true
			}
			e.FragmentDownloadedArchive(date, set, archiveType)
		}
		for _, v := range filtered {
			archiveType := pcommon.ArchiveType(v[0])
			date := v[1]
			e.DownloadArchive(date, set, archiveType)
		}
	}

	return nil
}

func (e *engine) DownloadArchive(date string, set *pcommon.SetJSON, at pcommon.ArchiveType) {
	e.Add(buildArchiveDownloader(date, set, at))
}

func (e *engine) FragmentDownloadedArchive(date string, set *pcommon.SetJSON, at pcommon.ArchiveType) error {
	archivePath := at.GetArchiveZipPath(date, set)
	stat, err := os.Stat(archivePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	if stat.ModTime().Add(time.Minute * 2).After(time.Now()) {
		return nil
	}

	tree, ok := pcommon.ArchivesIndex[at]
	if !ok {
		return fmt.Errorf("archive tree not found")
	}
	countFound := 0
	for _, col := range tree.Columns {
		if _, err := os.Stat(set.Settings.BuildArchiveFilePath(col.Asset, date, "zip")); err == nil {
			countFound++
		}
	}
	if countFound == len(tree.Columns) {
		return nil
	}

	e.Add(buildArchiveFragmenter(date, set, at))
	return nil
}

func (e *engine) StopSetRunners(set *pcommon.SetJSON) {
	e.CancelRunnersByArgs(map[string]interface{}{
		ARG_VALUE_SET: set,
	})
}
