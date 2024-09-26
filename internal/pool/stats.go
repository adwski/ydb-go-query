package pool

import (
	s "github.com/adwski/ydb-go-query/internal/stats"
)

type (
	stats struct {
		inUse_ s.Gauge
		idle_  s.Gauge
		ready_ s.Indicator
	}
)

func newStats(hi, lo int64) stats {
	return stats{
		inUse_: s.NewGauge(),
		idle_:  s.NewGauge(),
		ready_: s.NewIndicator(hi, lo),
	}
}

func (s *stats) inUse() s.Gauge {
	return s.inUse_
}

func (s *stats) idle() s.Gauge {
	return s.idle_
}

func (s *stats) ready() s.Indicator {
	return s.ready_
}

func (s *stats) updateReady() {
	s.ready_.Observe(s.idle_.Get() + s.inUse_.Get())
}
