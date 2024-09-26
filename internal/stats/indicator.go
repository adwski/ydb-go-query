package stats

import "sync/atomic"

type Indicator struct {
	v *atomic.Bool

	thresholdHi int64
	thresholdLo int64
}

func NewIndicator(hi, lo int64) Indicator {
	return Indicator{
		thresholdHi: hi,
		thresholdLo: lo,
		v:           &atomic.Bool{},
	}
}

func (i Indicator) Observe(val int64) {
	if i.v.Load() {
		if val <= i.thresholdLo {
			i.v.Swap(false)
		}
	} else {
		if val >= i.thresholdHi {
			i.v.Swap(true)
		}
	}
}

func (i Indicator) Get() bool {
	return i.v.Load()
}
