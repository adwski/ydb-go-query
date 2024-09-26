package stats

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCounter(t *testing.T) {
	c := NewCounter()

	assert.Equal(t, uint64(0), c.Get())

	c.Inc()
	assert.Equal(t, uint64(1), c.Get())

	c.Reset()
	assert.Equal(t, uint64(0), c.Get())
}

func TestCounterConcurrent(t *testing.T) {
	c := NewCounter()

	wg := sync.WaitGroup{}
	wg.Add(1000000)
	for i := 0; i < 1000000; i++ {
		go func() {
			c.Inc()
			wg.Done()
		}()
	}

	wg.Wait()

	assert.Equal(t, uint64(1000000), c.Get())

	c.Reset()
	assert.Equal(t, uint64(0), c.Get())
}
