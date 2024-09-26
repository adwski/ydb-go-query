package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndicator(t *testing.T) {
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
