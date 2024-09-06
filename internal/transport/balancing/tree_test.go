package balancing

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type conn struct {
	uid   uint64
	alive bool
}

func (c *conn) Alive() bool {
	return c.alive
}

func (c *conn) Close() error {
	return nil
}

func TestTreeFillAndGet(t *testing.T) {
	type args struct {
		lvl        int
		children   int
		numGetConn int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "3lvlTree",
			args: args{
				lvl:        3,
				children:   9,
				numGetConn: 30000,
			},
		},
		{
			name: "5lvlTree",
			args: args{
				lvl:        5,
				children:   5,
				numGetConn: 200000,
			},
		},
		{
			name: "10lvlTree",
			args: args{
				lvl:        10,
				children:   3,
				numGetConn: 1000000,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := createTree(t, tt.args.lvl)
			require.NoError(t, err)

			stats := make(map[uint64]int)

			fillTree(t, tree, func(pID uint64) *conn {
				c := &conn{
					alive: true,
				}
				for {
					uid := pID*1000 + uint64(rand.Intn(1000))
					if _, ok := stats[uid]; !ok {
						c.uid = uid
						break
					}
				}
				stats[c.uid] = 0

				return c
			}, tt.args.lvl, tt.args.children)

			// get conns from the tree
			mux := sync.Mutex{}
			wg := sync.WaitGroup{}
			wg.Add(tt.args.numGetConn)
			for i := 0; i < tt.args.numGetConn; i++ {
				go func() {
					got := tree.GetConn()
					require.NotNil(t, got)

					mux.Lock()
					stats[got.uid]++
					mux.Unlock()
					wg.Done()
				}()
			}

			wg.Wait()

			mx, mn := 0, tt.args.numGetConn
			for _, count := range stats {
				if count > mx {
					mx = count
				}
				if count < mn {
					mn = count
				}
			}

			assert.Equal(t, int(math.Pow(float64(tt.args.children), float64(tt.args.lvl))), len(stats),
				"connections amount should be equal to number of children at bottom level")
			assert.Greater(t, mn, 0, "all connections must've been used")
			assert.True(t, mx-1 == mn || mx == mn, "connection usage should be monotonous")

			t.Log("min hit:", mn, "max hit:", mx, "unique:", len(stats))
		})
	}
}

func createTree(t *testing.T, lvl int) (*Tree[*conn, conn], error) {
	t.Helper()

	levels := make([]Level, lvl)
	for i := 1; i < lvl; i++ {
		levels[i-1] = Level{
			Kind:   fmt.Sprintf("Level%d", i),
			Policy: PolicyKindRoundRobin,
		}
	}
	levels[lvl-1] = Level{
		Kind:   LevelKindConnection,
		Policy: PolicyKindRoundRobin,
	}

	return NewTree[*conn, conn](TreeConfig[*conn, conn]{
		Levels: levels,
	})
}

func fillTree(t *testing.T, tree *Tree[*conn, conn], connFunc func(uint64) *conn, lvl, numCh int) {
	t.Helper()

	var (
		genLvl func(lvl int, seq []int)
	)
	genLvl = func(lvl int, seq []int) {
		if lvl == 1 {
			ids := stringIDs(seq...)
			pID := pathID(seq...)
			err := tree.AddPath(Path[*conn, conn]{
				IDs: ids,
				ConnectionConfig: ConnectionConfig[*conn, conn]{
					ConnFunc:   func() (*conn, error) { return connFunc(pID), nil },
					ConnNumber: numCh,
				},
			})
			require.NoError(t, err)

			return
		}

		for i := 1; i <= numCh; i++ {
			genLvl(lvl-1, append(seq, i))
		}
	}

	genLvl(lvl, make([]int, 0, lvl))
}

func pathID(vals ...int) uint64 {
	mul := 1
	id := uint64(0)
	for i := len(vals) - 1; i >= 0; i-- {
		id += uint64(vals[i] * mul)
		mul *= 10
	}
	return id
}

func stringIDs(vals ...int) []string {
	out := make([]string, 0, len(vals))
	for _, val := range vals {
		out = append(out, strconv.Itoa(val))
	}
	return out
}
