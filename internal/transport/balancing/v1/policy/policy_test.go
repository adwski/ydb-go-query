package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	node struct {
		alive bool
		idx   int
	}
	test struct {
		name       string
		nodes      []*node
		wantIdx    int
		checkValid bool // just check if idx is in range
	}
)

func (n *node) Alive() bool {
	return n.alive
}

var (
	simpleTests = []test{
		{
			name: "0 ready",
			nodes: func() []*node {
				nodes := make([]*node, 10)
				for i := range nodes {
					nodes[i] = &node{idx: i}
				}
				nodes[0].alive = true
				return nodes
			}(),
			wantIdx: 0,
		},
		{
			name: "1 ready",
			nodes: func() []*node {
				nodes := make([]*node, 10)
				for i := range nodes {
					nodes[i] = &node{idx: i}
				}
				nodes[1].alive = true
				return nodes
			}(),
			wantIdx: 1,
		},
		{
			name: "none ready",
			nodes: func() []*node {
				nodes := make([]*node, 10)
				for i := range nodes {
					nodes[i] = &node{idx: i}
				}
				return nodes
			}(),
			wantIdx: -1,
		},
		{
			name: "all alive",
			nodes: func() []*node {
				nodes := make([]*node, 10)
				for i := range nodes {
					nodes[i] = &node{
						idx:   i,
						alive: true,
					}
				}
				return nodes
			}(),
		},
	}
)

type policy_[PT Egress[T], T any] interface {
	Get([]PT) PT
}

func testSimple(t *testing.T, pFunc func() policy_[*node, node], tests []test) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pFunc().Get(tt.nodes)

			switch {
			case tt.checkValid:
				require.NotNil(t, got)
				assert.True(t, got.idx >= 0 && got.idx <= 9)
			case tt.wantIdx >= 0:
				require.NotNil(t, got)
				assert.Equal(t, tt.wantIdx, got.idx)
			default:
				assert.Nil(t, got)
			}
		})
	}
}

func TestFirstReady(t *testing.T) {
	testSimple(t, func() policy_[*node, node] { return &FirstReady[*node, node]{} }, simpleTests)
}

func TestRandom(t *testing.T) {
	simpleTests[3].checkValid = true
	testSimple(t, func() policy_[*node, node] { return &Random[*node, node]{} }, simpleTests)
}

func TestRoundRobin(t *testing.T) {
	simpleTests[3].checkValid = false
	simpleTests[3].wantIdx = 1 // rr follows next node
	testSimple(t, func() policy_[*node, node] { return &RoundRobin[*node, node]{} }, simpleTests)
}

func TestRoundRobinCircular(t *testing.T) {
	nodes := make([]*node, 10)
	for i := range nodes {
		nodes[i] = &node{
			idx:   i,
			alive: true,
		}
	}

	policy := RoundRobin[*node, node]{}

	for i := 1; i < 21; i++ {
		got := policy.Get(nodes)
		require.NotNil(t, got)
		require.Equal(t, got.idx, i%10)
	}
}

func TestRoundRobinCircularConcurrent(t *testing.T) {
	nodes := make([]*node, 10)
	for i := range nodes {
		nodes[i] = &node{
			idx:   i,
			alive: true,
		}
	}

	policy := RoundRobin[*node, node]{}

	resCh := make(chan *node)

	go func() {
		for i := 0; i < 100000; i++ {
			go func() {
				got := policy.Get(nodes)
				resCh <- got
			}()
		}
	}()

	var stats [10]int

	for i := 0; i < 100000; i++ {
		got := <-resCh
		require.NotNil(t, got)
		assert.True(t, got.idx >= 0 && got.idx <= 9)

		stats[got.idx]++
	}

	t.Logf("hit stats: %v", stats)

	for i := 1; i < 10; i++ {
		assert.Equal(t, stats[0], stats[i])
	}
}
