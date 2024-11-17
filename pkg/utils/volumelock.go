package utils

import (
	"sync"
)

var (
	NamedOperationAlreadyExistsFmt = "Named operation already exists for volume %s"
)

type NamedLocks struct {
	locks map[string]bool
	mux   *sync.Mutex
}

func NewNamedLocks() *NamedLocks {
	return &NamedLocks{
		locks: make(map[string]bool),
		mux:   &sync.Mutex{},
	}
}

func (vl *NamedLocks) TryAcquire(name string) bool {
	vl.mux.Lock()
	defer vl.mux.Unlock()
	if _, exists := vl.locks[name]; exists {
		return false
	}
	vl.locks[name] = true
	return true
}

func (vl *NamedLocks) Release(name string) {
	vl.mux.Lock()
	defer vl.mux.Unlock()
	delete(vl.locks, name)
}
