package stats

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGauge(t *testing.T) {
	g := NewGauge()

	assert.Equal(t, int64(0), g.Get())

	g.Inc()
	assert.Equal(t, int64(1), g.Get())

	g.Dec()
	assert.Equal(t, int64(0), g.Get())
}

func TestGaugeConcurrent(t *testing.T) {
	g := NewGauge()

	wg := sync.WaitGroup{}
	wg.Add(1500001)
	for i := 0; i < 1000000; i++ {
		go func() {
			g.Inc()
			wg.Done()
		}()
	}
	for i := 0; i < 500001; i++ {
		go func() {
			g.Dec()
			wg.Done()
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(499999), g.Get())
}
