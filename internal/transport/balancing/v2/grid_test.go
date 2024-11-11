package v2

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGridFillAndGet(t *testing.T) {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gr := createGrid(t, tt.args.lvl, tt.args.children)

			ctx, cancel := context.WithCancel(context.Background())
			wgg := &sync.WaitGroup{}
			wgg.Add(1)
			go gr.Run(ctx, wgg)

			stats := make(map[uint64]int)

			fillGrid(t, gr, func(pID uint64) *conn {
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

			// get conns from the grid
			mux := sync.Mutex{}
			wg := &sync.WaitGroup{}
			wg.Add(tt.args.numGetConn)
			for i := 0; i < tt.args.numGetConn; i++ {
				go func() {
					got := gr.GetBalanced()
					require.NotNil(t, got)

					mux.Lock()
					stats[got.uid]++
					mux.Unlock()
					wg.Done()
				}()
			}

			wg.Wait()
			cancel()
			wgg.Wait()

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

func TestTreeGetAddDelConcurrent(t *testing.T) {
	const (
		delAfter    = 2000 * time.Millisecond
		delInterval = 1000 // msecs
		addInterval = time.Second

		testDuration = 10 * time.Second

		workerCount = 100
	)

	gr := createGrid(t, 3, 3)

	ctx, cancel := context.WithCancel(context.Background())
	wgg := &sync.WaitGroup{}
	wgg.Add(1)
	go gr.Run(ctx, wgg)

	addDel := func(IDs ...string) {
		errA := gr.AddEndpoint(IDs, func() (*conn, error) {
			return &conn{alive: true}, nil
		})
		require.NoError(t, errA)

		go func() {
			deletingAfter := delAfter - delInterval + time.Duration(rand.Intn(2*delInterval))*time.Millisecond
			time.Sleep(deletingAfter)
			errD := gr.DeleteEndpoint(IDs)
			require.NoError(t, errD)
		}()
	}

	addDel("a", "b")

	ctxT, cancelT := context.WithDeadline(context.Background(), time.Now().Add(testDuration))
	defer cancelT()

	wg := &sync.WaitGroup{}

	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctxT.Done():
				return
			case <-time.After(addInterval):
				addDel(strconv.Itoa(rand.Int()), strconv.Itoa(rand.Int()))
			}
		}
	}()

	gotConns := &atomic.Uint64{}
	for wkr := 0; wkr < workerCount; wkr++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctxT.Done():
					return
				default:
					conn_ := gr.GetBalanced()
					require.NotNil(t, conn_)
					gotConns.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	cancel()
	wgg.Wait()

	t.Log("got connections: ", gotConns.Load())
}

func fillGrid(tb testing.TB, gr *Grid[*conn, conn], connFunc func(uint64) *conn, lvl, numCh int) {
	tb.Helper()

	var (
		genLvl func(lvl int, seq []int)
	)
	genLvl = func(lvl int, seq []int) {
		if lvl == 1 {
			ids := stringIDs(seq...)
			pID := pathID(seq...)
			err := gr.AddEndpoint(ids, func() (*conn, error) { return connFunc(pID), nil })
			require.NoError(tb, err)

			return
		}

		for i := 1; i <= numCh; i++ {
			genLvl(lvl-1, append(seq, i))
		}
	}

	genLvl(lvl, make([]int, 0, lvl))
}

func createGrid(tb testing.TB, lvl, connNum int) *Grid[*conn, conn] {
	tb.Helper()

	levels := make([]Level, lvl)
	for i := 1; i < lvl; i++ {
		levels[i-1] = Level{
			Kind:   fmt.Sprintf("Level%d", i),
			Policy: PolicyKindRoundRobin,
		}
	}
	levels[lvl-1] = Level{
		Kind:   levelKindConnection,
		Policy: PolicyKindRoundRobin,
	}

	gr, err := newGrid[*conn, conn](Config[*conn, conn]{
		// ConnFunc: nil,
		treeCfg: treeConfig[*conn, conn]{
			levels: levels,
		},
		ConnectionsPerEndpoint: connNum,
	})

	require.NoError(tb, err)
	return gr
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
