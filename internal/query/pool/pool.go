package pool

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	localErrs "github.com/adwski/ydb-go-query/v1/internal/errors"
	"github.com/adwski/ydb-go-query/v1/internal/logger"
)

const (
	defaultCreateTimeout = 3 * time.Second
	defaultRecycleTick   = 2 * time.Second
	minCreateTimeout     = time.Second
	minItemLifetime      = 5 * time.Minute

	defaultCreateRetryDelayOnLocalErrors = time.Second
)

type (
	item[T any] interface {
		*T

		ID() uint64
		Alive() bool
		Close() error
	}

	Pool[PT item[T], T any] struct {
		logger logger.Logger

		createFunc func(context.Context, time.Duration) (PT, error)
		cancelFunc context.CancelFunc

		wg        *sync.WaitGroup
		closeOnce *sync.Once

		queue  chan PT
		tokens chan struct{}

		itemsExpire map[uint64]int64
		itemsMx     *sync.RWMutex

		createTimeout time.Duration
		itemLifetime  int64 // seconds
		recycleWindow int64 // seconds

		size uint

		itemRecycling bool
	}

	// Config holds pool configuration.
	Config[PT item[T], T any] struct {
		Logger logger.Logger

		// CreateFunc is used to create pool item.
		CreateFunc func(context.Context, time.Duration) (PT, error)

		// CreateTimeout limits runtime for CreateFunc.
		// This timeout cannot be less than a second (minCreateTimeout).
		// Default is 3 seconds (defaultCreateTimeout).
		CreateTimeout time.Duration

		// Lifetime specifies item lifetime after which it will be closed
		// and new item will be created instead.
		// 0 lifetime means item has infinite lifetime and item recycling
		// is not running.
		// Lifetime cannot be greater than 0 and less than 5 seconds (minItemLifetime).
		Lifetime time.Duration

		// RecycleWindow specifies time interval for item recycling:
		// [Lifetime-RecycleWindow;Lifetime+RecycleWindow]
		// This prevents items to all be recycled in the same time.
		RecycleWindow time.Duration

		// PoolSize specifies amount of items in pool.
		PoolSize uint
	}
)

func New[PT item[T], T any](ctx context.Context, cfg Config[PT, T]) *Pool[PT, T] {
	runCtx, cancel := context.WithCancel(ctx)

	if cfg.CreateTimeout < minCreateTimeout {
		cfg.CreateTimeout = defaultCreateTimeout
	}
	if cfg.Lifetime < minItemLifetime {
		cfg.Lifetime = 0 // infinite lifetime
	}

	pool := &Pool[PT, T]{
		logger:        cfg.Logger,
		size:          cfg.PoolSize,
		createTimeout: cfg.CreateTimeout,
		itemLifetime:  cfg.Lifetime.Microseconds() / 1000,
		recycleWindow: cfg.RecycleWindow.Microseconds() / 1000,

		itemRecycling: cfg.Lifetime != 0,

		createFunc: cfg.CreateFunc,
		cancelFunc: cancel,

		wg:        &sync.WaitGroup{},
		closeOnce: &sync.Once{},

		itemsExpire: make(map[uint64]int64),
		itemsMx:     &sync.RWMutex{},

		queue:  make(chan PT, cfg.PoolSize),
		tokens: make(chan struct{}, cfg.PoolSize),
	}

	// fill tokens
	for i := 0; i < int(cfg.PoolSize); i++ {
		pool.tokens <- struct{}{}
	}

	// start spawner
	pool.wg.Add(1)
	go pool.spawnItems(runCtx)

	if pool.itemRecycling {
		// start recycler
		pool.wg.Add(1)
		go pool.recycleItems(runCtx)
	}

	pool.logger.Trace("pool created", "size", pool.size)

	return pool
}

func (p *Pool[PT, T]) Close() error {
	p.closeOnce.Do(func() {
		p.cancelFunc()
		p.drain()
		p.wg.Wait()

		p.logger.Trace("pool closed")
	})

	return nil
}

func (p *Pool[PT, T]) Get(rCtx context.Context) PT {
getLoop:
	for {
		select {
		case itm := <-p.queue:
			if itm.Alive() {
				p.logger.Trace("item in use", "id", itm.ID())
				return itm
			}
			_ = itm.Close()

			select {
			case p.tokens <- struct{}{}:
			case <-rCtx.Done():
				break getLoop
			}
		case <-rCtx.Done():
			break getLoop
		}
	}

	return nil
}

func (p *Pool[PT, T]) Put(itm PT) {
	// check if alive
	if itm.Alive() {
		if !p.itemRecycling || !p.itemExpired(itm) {
			// alive and not expired
			// push item back and finish iteration
			p.queue <- itm
			p.logger.Trace("item returned to pool", "id", itm.ID())
			return
		}
	}
	p.logger.Trace("item recycled on returning", "id", itm.ID())
	// recycle
	_ = itm.Close()
	// push token
	p.tokens <- struct{}{}
}

func (p *Pool[PT, T]) spawnItems(ctx context.Context) {
	p.logger.Trace("pool spawner started")
	defer func() {
		p.wg.Done()
		p.logger.Trace("pool spawner exited")
	}()

spawnLoop:
	for {
		select {
		case <-ctx.Done():
			break spawnLoop
		case <-p.tokens:
		createLoop:
			for {
				p.wg.Add(1)
				itm, err := p.spawnItem(ctx)
				if err != nil {
					if errors.Is(err, localErrs.LocalFailureError{}) {
						// Local errors return instantly.
						// Sleep here a bit to prevent unnecessary flood of create attempts.
						time.Sleep(defaultCreateRetryDelayOnLocalErrors)
					}
					select {
					case <-ctx.Done():
						break spawnLoop
					default:
						continue createLoop
					}
				}

				// Ignoring ctx.Done() here and put item in queue anyway,
				// so it can be closed later by drain().
				p.queue <- itm

				break
			}
		}
	}
}

func (p *Pool[PT, T]) drain() {
drainLoop:
	for {
		select {
		case itm := <-p.queue:
			_ = itm.Close()
		default:
			break drainLoop
		}
	}
}

func (p *Pool[PT, T]) spawnItem(ctx context.Context) (PT, error) {
	defer p.wg.Done()

	itm, err := p.createFunc(ctx, p.createTimeout)
	if err != nil {
		p.logger.Debug("pool item create error", "error", err)

		return nil, err
	}

	if p.itemRecycling {
		p.setItemExpire(itm.ID())
	}

	return itm, nil
}

func (p *Pool[PT, T]) setItemExpire(id uint64) {
	p.itemsMx.Lock()
	defer p.itemsMx.Unlock()

	p.itemsExpire[id] = time.Now().Unix() + p.itemLifetime
}

func (p *Pool[PT, T]) getItemExpire(id uint64) int64 {
	p.itemsMx.RLock()
	defer p.itemsMx.RUnlock()

	return p.itemsExpire[id]
}

func (p *Pool[PT, T]) itemExpired(itm PT) bool {
	return p.getItemExpire(itm.ID())-p.recycleWindow+rand.Int63n(2*p.recycleWindow) < time.Now().Unix()
}

func (p *Pool[PT, T]) recycleItems(ctx context.Context) {
	p.logger.Trace("pool recycler started")
	defer func() {
		p.wg.Done()
		p.logger.Trace("pool recycler exited")
	}()

	ticker := time.NewTicker(defaultRecycleTick)
	defer ticker.Stop()

recycleLoop:
	for {
		// wait for tick
		select {
		case <-ctx.Done():
			break recycleLoop
		case <-ticker.C:
		}

		// get item from queue
		select {
		case <-ctx.Done():
			break recycleLoop
		case itm := <-p.queue:
			// check if alive
			if itm.Alive() && p.itemExpired(itm) {
				// alive and not expired
				// push item back and finish iteration
				p.queue <- itm
				break recycleLoop
			}
			p.logger.Trace("item recycled", "id", itm.ID())
			// recycle
			_ = itm.Close()
			// push token
			p.tokens <- struct{}{}
		}
	}
}
