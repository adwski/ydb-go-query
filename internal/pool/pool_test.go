package pool

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	localErrs "github.com/adwski/ydb-go-query/internal/errors"
	"github.com/adwski/ydb-go-query/internal/logger"
	"github.com/adwski/ydb-go-query/internal/logger/noop"
)

type itm struct {
	mx *sync.RWMutex

	id     uint64
	closed atomic.Int32
	alive  bool
}

func (i *itm) setAlive(alive bool) {
	i.mx.Lock()
	defer i.mx.Unlock()

	i.alive = alive
}

func (i *itm) Alive() bool {
	i.mx.RLock()
	defer i.mx.RUnlock()

	return i.alive
}

func (i *itm) Close() error {
	i.mx.Lock()
	defer i.mx.Unlock()

	i.closed.Add(1)

	return nil
}

func (i *itm) ID() uint64 {
	return i.id
}

func TestPool_GetPut(t *testing.T) {
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	pool := New[*itm, itm](runCtx, Config[*itm, itm]{
		Logger: logger.New(noop.NewLogger()),
		CreateFunc: func(ctx context.Context, createTimeout time.Duration) (*itm, error) {
			return &itm{
				mx:    &sync.RWMutex{},
				id:    rand.Uint64(),
				alive: true,
			}, nil
		},
		CreateTimeout: time.Second,
		PoolSize:      1,
	})

	var (
		itm_ *itm
		done = make(chan struct{})
	)

	// Get item from pool
	ctxGet, cancelGet := context.WithTimeout(context.Background(), time.Second)
	go func() {
		itm_ = pool.Get(ctxGet)
		done <- struct{}{}
	}()

	select {
	case <-done:
		// get must succeed
	case <-ctxGet.Done():
		t.Fatal("timeout getting item from pool")
	}
	cancelGet()

	// Get again
	ctxGet, cancelGet = context.WithTimeout(context.Background(), time.Second)
	go func() {
		_ = pool.Get(ctxGet)
		done <- struct{}{}
	}()

	select {
	case <-done:
		t.Fatal("get must time out")
	case <-ctxGet.Done():
		// get must time out
	}
	cancelGet()
	<-done

	if err := pool.Close(); err != nil {
		t.Fatal("close must not return error", err.Error())
	}

	// Put item back
	ctxPut, cancelPut := context.WithTimeout(context.Background(), time.Second)
	go func() {
		pool.Put(itm_)
		done <- struct{}{}
	}()

	select {
	case <-done:
		// put must not block
	case <-ctxPut.Done():
		t.Fatal("put must not block")
	}
	cancelPut()

	if err := pool.Close(); err != nil {
		t.Fatal("close must not return error", err.Error())
	}
}

func TestPool_GetPutConcurrent(t *testing.T) {
	const (
		runTime      = 10 * time.Second
		numWorkers   = 10
		aliveAtLeast = 1000 // msec
		aliveWindow  = 1000 // msec
		poolSize     = 100
	)

	itmMx := &sync.Mutex{}
	itms := make([]*itm, 0, poolSize*100)

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	pool := New[*itm, itm](runCtx, Config[*itm, itm]{
		Logger: logger.New(noop.NewLogger()),
		CreateFunc: func(ctx context.Context, createTimeout time.Duration) (*itm, error) {
			itm_ := &itm{
				mx:    &sync.RWMutex{},
				id:    rand.Uint64(),
				alive: true,
			}
			go func() {
				time.Sleep(time.Duration(aliveAtLeast+rand.Intn(aliveWindow)) * time.Millisecond)
				itm_.setAlive(false)
			}()

			itmMx.Lock()
			itms = append(itms, itm_)
			itmMx.Unlock()

			return itm_, nil
		},
		CreateTimeout: time.Second,
		PoolSize:      poolSize,
	})

	wg := &sync.WaitGroup{}

	worker := func(ctx context.Context, t *testing.T, wg *sync.WaitGroup) {
		t.Helper()

		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				func() {
					ctxGet, cancelGet := context.WithTimeout(context.Background(), time.Second)
					defer cancelGet()

					itm_ := pool.Get(ctxGet)
					if itm_ == nil {
						t.Error("itm is nil")
						return
					}
					pool.Put(itm_)
				}()
			}
		}
	}

	t.Log("spawning workers")
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker(runCtx, t, wg)
	}

	time.Sleep(runTime)
	runCancel()
	wg.Wait()
	if err := pool.Close(); err != nil {
		t.Fatal("close must not return error", err.Error())
	}

	t.Log("items created total:", len(itms))
	for _, itm_ := range itms {
		if closedCalls := itm_.closed.Load(); closedCalls != 1 {
			t.Fatal("itm has incorrect close ctr:", closedCalls)
		}
	}
}

func TestPool_Recycle(t *testing.T) {
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	itmMx := &sync.Mutex{}
	itms := make([]*itm, 0, 10)

	pool := New[*itm, itm](runCtx, Config[*itm, itm]{
		Logger: logger.New(noop.NewLogger()),
		CreateFunc: func(ctx context.Context, createTimeout time.Duration) (*itm, error) {
			itm_ := &itm{
				mx:    &sync.RWMutex{},
				id:    rand.Uint64(),
				alive: true,
			}

			itmMx.Lock()
			itms = append(itms, itm_)
			itmMx.Unlock()

			return itm_, nil
		},
		CreateTimeout: time.Second,
		RecycleWindow: time.Second,
		Lifetime:      5 * time.Second,
		PoolSize:      1,

		test: true,
	})

	time.Sleep(7 * time.Second)
	if err := pool.Close(); err != nil {
		t.Fatal("close must not return error", err.Error())
	}

	t.Log("items created total:", len(itms))
	if len(itms) != 2 {
		t.Error("looks like first item was not recycled")
	}
	for _, itm_ := range itms {
		if closedCalls := itm_.closed.Load(); closedCalls != 1 {
			t.Fatal("itm has incorrect close ctr:", closedCalls)
		}
	}
}

func TestPool_LocalErrorCreateRetry(t *testing.T) {
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	ctr := 0

	pool := New[*itm, itm](runCtx, Config[*itm, itm]{
		Logger: logger.New(noop.NewLogger()),
		CreateFunc: func(ctx context.Context, createTimeout time.Duration) (*itm, error) {
			ctr++
			return nil, localErrs.LocalFailureError{}
		}})

	time.Sleep(2500 * time.Millisecond)
	if err := pool.Close(); err != nil {
		t.Fatal("close must not return error", err.Error())
	}

	if ctr != 3 {
		t.Error("something wrong with create retry delay, ctr:", ctr)
	}
}
