package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndicator5060(t *testing.T) {
	i := NewIndicator(60, 50)

	assert.False(t, i.Get())

	i.Observe(59)
	assert.False(t, i.Get())

	i.Observe(60)
	assert.True(t, i.Get())

	i.Observe(61)
	assert.True(t, i.Get())

	i.Observe(55)
	assert.True(t, i.Get())

	i.Observe(50)
	assert.False(t, i.Get())

	i.Observe(55)
	assert.False(t, i.Get())

	i.Observe(20)
	assert.False(t, i.Get())

	i.Observe(70)
	assert.True(t, i.Get())

	i.Observe(51)
	assert.True(t, i.Get())
}

func TestIndicator0100(t *testing.T) {
	i := NewIndicator(100, 0)

	assert.False(t, i.Get())

	i.Observe(50)
	assert.False(t, i.Get())

	i.Observe(99)
	assert.False(t, i.Get())

	i.Observe(100)
	assert.True(t, i.Get())

	i.Observe(50)
	assert.True(t, i.Get())

	i.Observe(1)
	assert.True(t, i.Get())

	i.Observe(0)
	assert.False(t, i.Get())

	i.Observe(50)
	assert.False(t, i.Get())

	i.Observe(99)
	assert.False(t, i.Get())

	i.Observe(100)
	assert.True(t, i.Get())

	i.Observe(50)
	assert.True(t, i.Get())
}
