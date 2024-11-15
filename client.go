package ydbgoquery

import (
	"context"
	"errors"
	"sync"

	"github.com/adwski/ydb-go-query/internal/discovery"
	"github.com/adwski/ydb-go-query/internal/logger"
	"github.com/adwski/ydb-go-query/internal/query"
	"github.com/adwski/ydb-go-query/internal/transport"
	balancing "github.com/adwski/ydb-go-query/internal/transport/balancing/v4"
	"github.com/adwski/ydb-go-query/internal/transport/dispatcher"
	qq "github.com/adwski/ydb-go-query/query"
)

var (
	ErrNoInitialNodes           = errors.New("no initial nodes was provided")
	ErrDBEmpty                  = errors.New("db is empty")
	ErrDiscoveryTransportCreate = errors.New("discovery transport create error")
)

func Open(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	client, err := newClient(ctx, &cfg, opts...)
	if err != nil {
		return nil, err
	}

	var runCtx context.Context
	runCtx, client.cancel = context.WithCancel(ctx)

	client.querySvc = query.NewService(runCtx, query.Config{
		Logger:    client.logger,
		Transport: client.dispatcher.Transport(),

		CreateTimeout:          cfg.sessionCreateTimeout,
		PoolSize:               cfg.poolSize,
		PoolReadyThresholdHigh: cfg.poolReadyHi,
		PoolReadyThresholdLow:  cfg.poolReadyLo,
	})

	client.queryCtx = qq.NewCtx(client.logger, client.querySvc, cfg.txSettings, cfg.queryTimeout)

	client.wg.Add(1)
	go client.dispatcher.Run(runCtx, client.wg)

	client.wg.Add(1)
	go client.discoverySvc.Run(runCtx, client.wg)

	if cfg.auth != nil {
		client.wg.Add(1)
		go cfg.auth.Run(runCtx, client.wg)
	}

	return client, nil
}

type (
	authRunner interface {
		transport.Authenticator

		Run(ctx context.Context, wg *sync.WaitGroup)
	}
	Client struct {
		dispatcher *dispatcher.Dynamic

		discoverySvc *discovery.Service
		querySvc     *query.Service

		queryCtx *qq.Ctx

		wg     *sync.WaitGroup
		cancel context.CancelFunc

		logger logger.Logger
	}
)

func (c *Client) QueryCtx() *qq.Ctx {
	return c.queryCtx
}

func (c *Client) Close() {
	c.cancel()
	_ = c.querySvc.Close()
	c.wg.Wait()
}

func (c *Client) Ready() bool {
	return c.querySvc.Ready()
}

func newClient(ctx context.Context, cfg *Config, opts ...Option) (*Client, error) {
	if len(cfg.InitialNodes) == 0 {
		return nil, ErrNoInitialNodes
	}
	if len(cfg.DB) == 0 {
		return nil, ErrDBEmpty
	}

	cfg.setDefaults()

	for _, opt := range opts {
		if err := opt(ctx, cfg); err != nil {
			return nil, err
		}
	}

	tr, err := dispatcher.NewStatic(ctx, cfg.InitialNodes, cfg.transportCredentials, cfg.auth, cfg.DB)
	if err != nil {
		return nil, errors.Join(ErrDiscoveryTransportCreate, err)
	}

	discoverySvc := discovery.NewService(discovery.Config{
		Logger:     cfg.logger,
		DB:         cfg.DB,
		Transport:  tr.Transport(),
		DoAnnounce: true,
	})

	dispatcherCfg := dispatcher.Config{
		Logger:    cfg.logger,
		InitNodes: cfg.InitialNodes,
		DB:        cfg.DB,
		Balancing: balancing.Config{
			LocationPreference: cfg.locationPreference,
			ConnsPerEndpoint:   cfg.connectionsPerEndpoint,
			IgnoreLocations:    false,
		},
		TransportCredentials: cfg.transportCredentials,
		Auth:                 cfg.auth,

		EndpointsProvider: discoverySvc,
	}

	c := &Client{
		logger: cfg.logger,

		dispatcher:   dispatcher.NewDynamic(dispatcherCfg),
		discoverySvc: discoverySvc,

		wg: &sync.WaitGroup{},
	}

	return c, nil
}
