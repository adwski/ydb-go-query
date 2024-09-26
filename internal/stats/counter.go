package stats

import "sync/atomic"

type Counter struct {
	v *atomic.Uint64
}

func NewCounter() Counter {
	return Counter{v: &atomic.Uint64{}}
}

func (c Counter) Inc() {
	c.v.Add(1)
}

func (c Counter) Reset() {
	c.v.Store(0)
}

func (c Counter) Get() uint64 {
	return c.v.Load()
}
