package stats

import "sync/atomic"

type Gauge struct {
	v *atomic.Int64
}

func NewGauge() *Gauge {
	return &Gauge{v: &atomic.Int64{}}
}

func (g Gauge) Inc() {
	g.v.Add(1)
}

func (g Gauge) Dec() {
	g.v.Add(-1)
}

func (g Gauge) Get() int64 {
	return g.v.Load()
}
