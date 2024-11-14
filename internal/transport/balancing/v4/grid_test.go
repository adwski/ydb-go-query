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
	loc   string
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

func TestLocationFallback(t *testing.T) {
	balancer := NewGrid[*conn, conn](Config{
		ConnsPerEndpoint:   4,
		LocationPreference: []string{"aaa", "bbb", "ccc"},
	})

	locMp := make(map[string][]*conn, 20)

	require.NoError(t, balancer.Add("aaa", func() (*conn, error) {
		conn_ := &conn{alive: true, loc: "aaa"}
		locMp["aaa"] = append(locMp["aaa"], conn_)
		return conn_, nil
	}))
	require.NoError(t, balancer.Add("bbb", func() (*conn, error) {
		conn_ := &conn{alive: true, loc: "bbb"}
		locMp["bbb"] = append(locMp["bbb"], conn_)
		return conn_, nil
	}))
	require.NoError(t, balancer.Add("ccc", func() (*conn, error) {
		conn_ := &conn{alive: true, loc: "ccc"}
		locMp["ccc"] = append(locMp["ccc"], conn_)
		return conn_, nil
	}))
	require.NoError(t, balancer.Add("ddd", func() (*conn, error) {
		conn_ := &conn{alive: true, loc: "ddd"}
		locMp["ddd"] = append(locMp["ddd"], conn_)
		return conn_, nil
	}))

	getConns := func(t *testing.T, loc string) {
		t.Helper()

		for range len(locMp) {
			conn_ := balancer.GetConn()
			require.NotNil(t, conn_)
			require.Equal(t, loc, conn_.loc)
		}
	}

	setAlive := func(t *testing.T, loc string, alive bool) {
		t.Helper()

		for _, conn_ := range locMp[loc] {
			conn_.alive = alive
		}
	}

	// first location should be used
	getConns(t, "aaa")
	// set aaa to be not alive
	setAlive(t, "aaa", false)
	// second location should be used
	getConns(t, "bbb")
	// set aaa to be not alive
	setAlive(t, "bbb", false)
	// third location should be used
	getConns(t, "ccc")
	// set ccc to be not alive
	setAlive(t, "ccc", false)
	// forth location should be used
	getConns(t, "ddd")
	// set ddd to be not alive
	setAlive(t, "ddd", false)
	// no connections should be available
	for range len(locMp) {
		require.Nil(t, balancer.GetConn())
	}
	// set ddd to be not alive
	setAlive(t, "ddd", true)
	// forth location should be used
	getConns(t, "ddd")
	// set ccc to alive
	setAlive(t, "ccc", true)
	// third location should be used
	getConns(t, "ccc")
	// set bbb to be not alive
	setAlive(t, "bbb", true)
	// second location should be used
	getConns(t, "bbb")
	// set aaa to be not alive
	setAlive(t, "aaa", true)
	// first location should be used
	getConns(t, "aaa")
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

			// get connsPerEndpoint from the tree
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
		name   string
		config Config
	}{
		{
			name: "ignore location",
			config: Config{
				ConnsPerEndpoint: 2,
			},
		},
		{
			name: "do not ignore location",
			config: Config{
				ConnsPerEndpoint:   2,
				LocationPreference: []string{"#placeholder#"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seed := maphash.MakeSeed()

			balancer := NewGrid[*conn, conn](tt.config)

			var (
				newConns, delConns            atomic.Uint64
				createFail, delFail, nilConns atomic.Uint64
			)

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
				if errAdd != nil {
					createFail.Add(1)

					return
				}

				wg.Add(1)
				go func() {
					defer wg.Done()

					deletingAfter := delAfter - delInterval + time.Duration(rand.Intn(2*delInterval))*time.Millisecond
					time.Sleep(deletingAfter)
					errDel := balancer.Delete(IDs[0], connID)
					if errDel != nil {
						delFail.Add(1)
					} else {
						delConns.Add(1)
					}
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
							if conn_ == nil {
								nilConns.Add(1)
							} else {
								gotConns.Add(1)
							}
						}
					}
				}()
			}

			wg.Wait()
			t.Log("added new: ", newConns.Load())
			t.Log("deleted: ", delConns.Load())
			t.Log("got connections: ", gotConns.Load())
			t.Log("create fail: ", createFail.Load())
			t.Log("delete fail: ", delFail.Load())
			t.Log("get fail: ", nilConns.Load())
			if createFail.Load() > 0 || delFail.Load() > 0 || nilConns.Load() > 0 {
				t.Fail()
			}
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
