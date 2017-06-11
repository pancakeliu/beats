package prospector

import (
	// "io/ioutil"
	"os"
	// "strings"
	"path/filepath"
	"sync"

	"github.com/elastic/beats/filebeat/harvester"
	"github.com/elastic/beats/filebeat/harvester/reader"
	"github.com/elastic/beats/libbeat/logp"
	uuid "github.com/satori/go.uuid"
)

type harvesterRegistry struct {
	sync.Mutex
	harvesters map[uuid.UUID]*harvester.Harvester
	wg         sync.WaitGroup
}

func newHarvesterRegistry() *harvesterRegistry {
	return &harvesterRegistry{
		harvesters: map[uuid.UUID]*harvester.Harvester{},
	}
}

func (hr *harvesterRegistry) add(h *harvester.Harvester) {
	hr.Lock()
	defer hr.Unlock()
	hr.harvesters[h.ID] = h
}

func (hr *harvesterRegistry) remove(h *harvester.Harvester) {
	hr.Lock()
	defer hr.Unlock()

	_, ok := hr.harvesters[h.ID]
	if ok {
		delete(hr.harvesters, h.ID)
	}
}

func (hr *harvesterRegistry) Stop() {
	hr.Lock()
	for _, hv := range hr.harvesters {
		hr.wg.Add(1)
		go func(h *harvester.Harvester) {
			hr.wg.Done()
			h.Stop()
		}(hv)
	}
	hr.Unlock()
	hr.waitForCompletion()
}

func (hr *harvesterRegistry) stopRemove(prospectorer Prospectorer) *harvester.Harvester {
	hr.Lock()
	defer hr.Unlock()

	var del_harv *harvester.Harvester
	del_harv = nil

	for _, hv := range hr.harvesters {

		if hv.State.Finished == true {
			logp.Info("hvState.Finished == true")
			continue
		}

		token := false
		p := prospectorer.(*ProspectorLog)
		for path, info := range p.GetFiles() {
			var err error
			path, err = filepath.Abs(path)
			if err != nil {
				logp.Err("could not fetch abs path for file %s: %s", path, err)
			}
			token = os.SameFile(hv.State.Fileinfo, info)
			if token == true {
				logp.Debug("%s is same as %s", hv.State.Fileinfo.Name(), info.Name())
				del_harv = nil
				break
			}
		}

		// has been removed
		if token == false {
			hv.Stop()
			del_harv = hv
			break
		}
	}
	return del_harv
}

func (hr *harvesterRegistry) StopRemove(prospectorer Prospectorer) {
	del_harv := hr.stopRemove(prospectorer)
	if del_harv != nil {
		hr.remove(del_harv)
	}
	// logp.Info("StopRemove() over")
}

func (hr *harvesterRegistry) waitForCompletion() {
	hr.wg.Wait()
}

func (hr *harvesterRegistry) start(h *harvester.Harvester, r reader.Reader) {

	hr.wg.Add(1)
	hr.add(h)
	go func() {

		defer func() {
			hr.remove(h)
			hr.wg.Done()
		}()
		// Starts harvester and picks the right type. In case type is not set, set it to default (log)
		h.Harvest(r)
	}()
}

func (hr *harvesterRegistry) len() uint64 {
	hr.Lock()
	defer hr.Unlock()
	return uint64(len(hr.harvesters))
}
