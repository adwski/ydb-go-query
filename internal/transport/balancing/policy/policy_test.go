package policy

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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

var (
	oneOrNoneAlive = []test{
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
	}
)

func (n *node) Alive() bool {
	return n.alive
}

func TestFirstReady(t *testing.T) {
	tests := append(oneOrNoneAlive, []test{
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
			wantIdx: 0,
		},
	}...)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := FirstReady[*node, node]{}

			got := policy.Get(tt.nodes)

			if tt.wantIdx >= 0 {
				require.NotNil(t, got)
				assert.Equal(t, tt.wantIdx, got.idx)
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

func TestRandom(t *testing.T) {
	tests := append(oneOrNoneAlive, []test{
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
			checkValid: true,
		},
	}...)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := Random[*node, node]{}

			got := policy.Get(tt.nodes)

			if tt.checkValid {
				require.NotNil(t, got)
				assert.True(t, got.idx >= 0 && got.idx <= 9)
			} else if tt.wantIdx >= 0 {
				require.NotNil(t, got)
				assert.Equal(t, tt.wantIdx, got.idx)
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

func TestRoundRobin(t *testing.T) {
	tests := append(oneOrNoneAlive, []test{
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
			wantIdx: 1, // rr follows next node
		},
	}...)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := RoundRobin[*node, node]{}

			got := policy.Get(tt.nodes)

			if tt.wantIdx >= 0 {
				require.NotNil(t, got)
				assert.Equal(t, tt.wantIdx, got.idx)
			} else {
				assert.Nil(t, got)
			}
		})
	}
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
