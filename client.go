package ydbgoquery

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/query"
	"github.com/adwski/ydb-go-query/v1/internal/transport"
	"github.com/adwski/ydb-go-query/v1/internal/transport/balancing/grid"
	"github.com/adwski/ydb-go-query/v1/internal/transport/dispatcher"
	qq "github.com/adwski/ydb-go-query/v1/query"
)

const (
	defaultConnectionsPerEndpoint = 2
)

var (
	ErrNoInitialNodes = errors.New("no initial nodes was provided")
	ErrCfgDispatcher  = errors.New("unable to configure dispatcher")
	ErrDBEmpty        = errors.New("db is empty")
)

func Open(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	client, err := newClient(ctx, &cfg, opts...)
	if err != nil {
		return nil, err
	}

	var runCtx context.Context
	runCtx, client.cancel = context.WithCancel(ctx)

	if err = client.dispatcher.Init(runCtx); err != nil {
		client.cancel()
		return nil, errors.Join(ErrCfgDispatcher, err)
	}

	client.querySvc = query.NewService(runCtx, query.Config{
		Logger:        client.logger,
		Transport:     client.dispatcher.Transport(),
		CreateTimeout: client.sessionCreateTimeout,
		PoolSize:      client.poolSize,
	})

	client.queryCtx = qq.NewCtx(client.logger, client.querySvc, cfg.txSettings)

	client.wg.Add(1)
	go client.dispatcher.Run(runCtx, client.wg)

	client.wg.Add(1)
	go cfg.auth.Run(runCtx, client.wg)

	return client, nil
}

type (
	authRunner interface {
		transport.Authenticator

		Run(ctx context.Context, wg *sync.WaitGroup)
	}
	Client struct {
		logger logger.Logger

		dispatcher *dispatcher.Dispatcher
		querySvc   *query.Service

		queryCtx *qq.Ctx

		wg     *sync.WaitGroup
		cancel context.CancelFunc

		sessionCreateTimeout time.Duration
		poolSize             uint
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

	crdCfg := dispatcher.Config{
		Logger:    cfg.logger,
		InitNodes: cfg.InitialNodes,
		DB:        cfg.DB,
		GridConfig: grid.Config{
			ConnectionsPerEndpoint: defaultConnectionsPerEndpoint,
		},
		TransportCredentials: cfg.transportCredentials,
		Auth:                 cfg.auth,
	}

	crd := dispatcher.New(crdCfg)

	c := &Client{
		logger:               cfg.logger,
		sessionCreateTimeout: cfg.sessionCreateTimeout,
		poolSize:             cfg.poolSize,

		dispatcher: crd,
		wg:         &sync.WaitGroup{},
	}

	return c, nil
}
