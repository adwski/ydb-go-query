package v3

import (
	"context"
	"hash/maphash"
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

type conn struct {
	uid   uint64
	id    uint64
	alive bool
}

func (c *conn) Alive() bool {
	return c.alive
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) ID() uint64 {
	return c.id
}

func TestAddDel(t *testing.T) {
	type args struct {
		lvl          int
		addEndpoint  [2]string
		delEndpoint  [2]string
		locationPref []string
		connNum      int
		alive        bool
	}
	tests := []struct {
		name        string
		args        args
		addErr      error
		delErr      error
		nilAfterAdd bool
		nilAfterDel bool
	}{
		{
			name: "alive",
			args: args{
				lvl:         3,
				addEndpoint: [2]string{"1", "2"},
				delEndpoint: [2]string{"1", "2"},
				connNum:     1,
				alive:       true,
			},
			nilAfterAdd: false,
			nilAfterDel: true,
		},
		{
			name: "not alive",
			args: args{
				lvl:         3,
				addEndpoint: [2]string{"1", "2"},
				delEndpoint: [2]string{"1", "2"},
				connNum:     10,
				alive:       false,
			},
			nilAfterAdd: true,
			nilAfterDel: true,
		},
		{
			name: "del wrong loc",
			args: args{
				lvl:          3,
				addEndpoint:  [2]string{"1", "2"},
				delEndpoint:  [2]string{"3", "4"},
				locationPref: []string{"1"},
				connNum:      10,
				alive:        true,
			},
			nilAfterAdd: false,
			nilAfterDel: false,
			delErr:      ErrUnknownLocation,
		},
		{
			name: "del wrong id default loc",
			args: args{
				lvl:         3,
				addEndpoint: [2]string{"1", "2"},
				delEndpoint: [2]string{"3", "4"},
				connNum:     10,
				alive:       true,
			},
			nilAfterAdd: false,
			nilAfterDel: false,
			delErr:      ErrNoSuchID,
		},
		{
			name: "del wrong id",
			args: args{
				lvl:         3,
				addEndpoint: [2]string{"1", "2"},
				delEndpoint: [2]string{"1", "3"},
				connNum:     10,
				alive:       true,
			},
			nilAfterAdd: false,
			nilAfterDel: false,
			delErr:      ErrNoSuchID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seed := maphash.MakeSeed()

			balancer := NewGrid[*conn, conn](Config{
				LocationPreference: tt.args.locationPref,
				ConnsPerEndpoint:   2,
			})

			err := balancer.Add(tt.args.addEndpoint[0], func() (*conn, error) {
				return &conn{
					alive: tt.args.alive,
					id:    maphash.String(seed, tt.args.addEndpoint[1]),
				}, nil
			})
			require.NoError(t, err)

			got := balancer.GetConn()
			require.Equal(t, tt.nilAfterAdd, got == nil)

			err = balancer.Delete(tt.args.delEndpoint[0], maphash.String(seed, tt.args.delEndpoint[1]))
			require.ErrorIs(t, err, tt.delErr)

			got = balancer.GetConn()
			require.Equal(t, tt.nilAfterDel, got == nil)
		})
	}
}

func TestTreeFillAndGet(t *testing.T) {
	type args struct {
		children   int
		numGetConn int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "9endpoints_9conns_30000getConns",
			args: args{
				children:   9,
				numGetConn: 30000,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			balancer := NewGrid[*conn, conn](Config{
				ConnsPerEndpoint: tt.args.children,
				IgnoreLocations:  true, // all connections will be in default location
			})

			stats := make(map[uint64]int)

			fillTree(t, balancer, func(pID uint64) *conn {
				c := &conn{
					alive: true,
					id:    pID,
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
			}, tt.args.children)

			t.Log("tree is filled")

			// get conns from the tree
			mux := sync.Mutex{}
			wg := sync.WaitGroup{}
			wg.Add(tt.args.numGetConn)
			for i := 0; i < tt.args.numGetConn; i++ {
				go func() {
					got := balancer.GetConn()
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

			assert.Equal(t, int(math.Pow(float64(tt.args.children), float64(3))), len(stats),
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
		delInterval = 500 // msecs
		addInterval = 100 * time.Millisecond

		testDuration = 10 * time.Second

		workerCount = 100
	)

	tests := []struct {
		name           string
		ignoreLocation bool
	}{
		{
			name:           "ignore location",
			ignoreLocation: true,
		},
		{
			name: "do not ignore location",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seed := maphash.MakeSeed()

			balancer := NewGrid[*conn, conn](Config{
				ConnsPerEndpoint:   2,
				IgnoreLocations:    tt.ignoreLocation,
				LocationPreference: []string{"#placeholder#"},
			})

			newConns := &atomic.Uint64{}
			delConns := &atomic.Uint64{}

			wg := &sync.WaitGroup{}

			addDel := func(IDs ...string) {
				newConns.Add(1)
				connID := maphash.String(seed, IDs[0]+IDs[1])
				errAdd := balancer.Add(IDs[0], func() (*conn, error) {
					return &conn{
						alive: true,
						id:    connID,
					}, nil
				})
				require.NoError(t, errAdd)

				wg.Add(1)
				go func() {
					defer wg.Done()

					deletingAfter := delAfter - delInterval + time.Duration(rand.Intn(2*delInterval))*time.Millisecond
					time.Sleep(deletingAfter)
					errDel := balancer.Delete(IDs[0], connID)
					require.NoError(t, errDel)
					delConns.Add(1)
				}()
			}

			addDel("a", "b")

			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(testDuration))
			defer cancel()

			wg.Add(1)
			go func() {
				defer wg.Done()
				uniq := make(map[string]struct{})
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(addInterval):
						for {
							loc, endp := strconv.Itoa(rand.Int()), strconv.Itoa(rand.Int())
							if _, ok := uniq[loc+endp]; !ok {
								uniq[loc+endp] = struct{}{}
								addDel(loc, endp)
								break
							}
						}
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
						case <-ctx.Done():
							return
						default:
							conn_ := balancer.GetConn()
							require.NotNil(t, conn_)
							gotConns.Add(1)
						}
					}
				}()
			}

			wg.Wait()
			t.Log("added new: ", newConns.Load())
			t.Log("deleted: ", delConns.Load())
			t.Log("got connections: ", gotConns.Load())
		})
	}
}

func fillTree(tb testing.TB, balancer *Grid[*conn, conn], connFunc func(uint64) *conn, numCh int) {
	tb.Helper()

	var (
		genLvl func(lvl int, seq []int)
	)
	genLvl = func(lvl int, seq []int) {
		if lvl == 1 {
			pID := pathID(seq...)
			err := balancer.Add(strconv.Itoa(seq[0]), func() (*conn, error) {
				return connFunc(pID), nil
			})
			require.NoError(tb, err)

			return
		}

		for i := 1; i <= numCh; i++ {
			genLvl(lvl-1, append(seq, i))
		}
	}

	genLvl(3, make([]int, 0, 3))
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
