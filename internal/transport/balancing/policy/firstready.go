package policy

type FirstReady[PT Egress[T], T any] struct {
}

func NewFirstReady[PT Egress[T], T any]() *FirstReady[PT, T] {
	return &FirstReady[PT, T]{}
}

func (fr *FirstReady[PT, T]) Get(egresses []PT) PT {
	// look for first ready conn
	for _, eg := range egresses {
		if eg.Alive() {
			return eg
		}
	}

	return nil
}