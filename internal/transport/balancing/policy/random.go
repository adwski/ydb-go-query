package policy

import (
	"math/rand"
)

type Random[PT Egress[T], T any] struct {
	*FirstReady[PT, T]
}

func NewRandom[PT Egress[T], T any]() *Random[PT, T] {
	return &Random[PT, T]{}
}

func (rnd *Random[PT, T]) Get(egresses []PT) PT {
	// select randomly
	eg := egresses[rand.Intn(len(egresses))]
	if eg.Alive() {
		return eg
	}

	// fallback to first ready conn
	return rnd.FirstReady.Get(egresses)
}
