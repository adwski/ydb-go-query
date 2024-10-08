package stats

import (
	"sync"
)

type Indicator struct {
	mx sync.Mutex
	v  bool

	thresholdHi int64
	thresholdLo int64
}

func NewIndicator(hi, lo int64) *Indicator {
	return &Indicator{
		thresholdHi: hi,
		thresholdLo: lo,
	}
}

func (i *Indicator) Observe(val int64) {
	i.mx.Lock()
	defer i.mx.Unlock()

	if i.v {
		if val <= i.thresholdLo {
			i.v = false
		}
	} else if val >= i.thresholdHi {
		i.v = true
	}
}

func (i *Indicator) Get() bool {
	i.mx.Lock()
	defer i.mx.Unlock()

	return i.v
}
