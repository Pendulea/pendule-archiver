package engine

import (
	"sync"

	pcommon "github.com/pendulea/pendule-common"
)

type WorkingSets map[string]*pcommon.SetJSON

var mu = sync.RWMutex{}

func (s *WorkingSets) Find(id string) *pcommon.SetJSON {
	v, exist := (*s)[id]
	if !exist {
		return nil
	}
	return v
}

func (s *WorkingSets) Add(set *pcommon.SetJSON) *pcommon.SetJSON {
	id := set.Pair.BuildSetID()
	mu.Lock()
	(*s)[id] = set
	mu.Unlock()
	return set
}

func (s *WorkingSets) Remove(id string) {
	mu.Lock()
	delete(*s, id)
	mu.Unlock()
}
