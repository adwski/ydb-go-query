package policy

// FirstReady returns first alive egress node starting from beginning.
// If no nodes are alive, it returns nil.
type FirstReady[PT Egress[T], T any] struct {
}

func NewFirstReady[PT Egress[T], T any]() *FirstReady[PT, T] {
	return &FirstReady[PT, T]{}
}

func (fr *FirstReady[PT, T]) Get(egresses []PT, _ []int) PT {
	// look for first ready conn
	for _, eg := range egresses {
		if eg.Alive() {
			return eg
		}
	}

	return nil
}
