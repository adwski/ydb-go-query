package policy

import (
	"sync/atomic"
)

type (
	RoundRobin[PT Egress[T], T any] struct {
		idx atomic.Uint32
	}
)

func NewRoundRobin[PT Egress[T], T any]() *RoundRobin[PT, T] {
	return &RoundRobin[PT, T]{}
}

func (c *RoundRobin[PT, T]) Get(egresses []PT) PT {
	defer func() {
		// ensure rr index is in bounds
		for old := c.idx.Load(); old >= uint32(len(egresses)); old = c.idx.Load() {
			if c.idx.CompareAndSwap(old, old%uint32(len(egresses))) {
				break
			}
		}
	}()

	// get next alive egress
	for i := 1; i < len(egresses); i++ {
		eg := egresses[c.idx.Add(1)%uint32(len(egresses))]
		if eg.Alive() {
			return eg
		}
	}
	return nil
}
