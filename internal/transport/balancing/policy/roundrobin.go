package policy

import (
	"sync"
)

type (
	// RoundRobin returns next alive egress node. It keeps track
	// of previously chosen node index, so it can determine next one.
	//
	// Node index is guarded by mutex.
	RoundRobin[PT Egress[T], T any] struct {
		mx  sync.Mutex
		idx int
	}
)

func NewRoundRobin[PT Egress[T], T any]() *RoundRobin[PT, T] {
	return &RoundRobin[PT, T]{}
}

func (c *RoundRobin[PT, T]) Get(egresses []PT) PT {
	c.mx.Lock()
	defer c.mx.Unlock()

	// get next alive egress
	for i := 0; i < len(egresses); i++ {
		c.idx = (c.idx + 1) % len(egresses)
		eg := egresses[c.idx]
		if eg.Alive() {
			return eg
		}
	}

	return nil
}
