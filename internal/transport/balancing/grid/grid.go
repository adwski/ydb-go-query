package grid

import (
	"context"
	"errors"

	localErrs "github.com/adwski/ydb-go-query/v1/internal/errors"
	"github.com/adwski/ydb-go-query/v1/internal/transport"
	"github.com/adwski/ydb-go-query/v1/internal/transport/balancing"
	"github.com/adwski/ydb-go-query/v1/internal/xcontext"

	"google.golang.org/grpc"
)

const (
	defaultConnectionsPerEndpoint = 2
)

var (
	ErrCreate        = errors.New("unable to initialize grid")
	ErrNoConnections = errors.New("no alive connections available")
)

type (
	Grid struct {
		*balancing.Tree[*transport.Connection, transport.Connection]
		connsPerEndpoint int
	}
	Config struct {
		ConnectionsPerEndpoint int
	}
	OneEndpointConfig struct {
		ConnFunc          func() (*transport.Connection, error)
		ConnectionsNumber int
	}
)

func (c *Config) check() {
	if c.ConnectionsPerEndpoint < 1 {
		c.ConnectionsPerEndpoint = defaultConnectionsPerEndpoint
	}
}

func NewWithOneEndpoint(cfg OneEndpointConfig) *Grid {
	if cfg.ConnectionsNumber < 1 {
		cfg.ConnectionsNumber = defaultConnectionsPerEndpoint
	}
	grid, _ := newGrid(Config{}, balancing.TreeConfig[*transport.Connection, transport.Connection]{
		Levels: []balancing.Level{
			{
				Kind:   balancing.LevelKindConnection,
				Policy: balancing.PolicyKindRoundRobin,
			},
		},
		ConnectionConfig: &balancing.ConnectionConfig[*transport.Connection, transport.Connection]{
			ConnNumber: cfg.ConnectionsNumber,
			ConnFunc:   cfg.ConnFunc,
		},
	})

	return grid
}

func NewWithTwoLevels(cfg Config) *Grid {
	cfg.check()
	grid, _ := newGrid(cfg, balancing.TreeConfig[*transport.Connection, transport.Connection]{
		Levels: []balancing.Level{
			{
				Kind:   balancing.LevelKindEndpoint,
				Policy: balancing.PolicyKindRoundRobin,
			},
			{
				Kind:   balancing.LevelKindConnection,
				Policy: balancing.PolicyKindRoundRobin,
			},
		},
	})

	return grid
}

func NewWithThreeLevels(cfg Config) *Grid {
	grid, _ := newGrid(cfg, balancing.TreeConfig[*transport.Connection, transport.Connection]{
		Levels: []balancing.Level{
			{
				Kind:   balancing.LevelKindLocation,
				Policy: balancing.PolicyKindFirstReady,
			},
			{
				Kind:   balancing.LevelKindEndpoint,
				Policy: balancing.PolicyKindRoundRobin,
			},
			{
				Kind:   balancing.LevelKindConnection,
				Policy: balancing.PolicyKindRoundRobin,
			},
		},
	})

	return grid
}

func newGrid(gridCfg Config, treeCfg balancing.TreeConfig[*transport.Connection, transport.Connection]) (*Grid, error) {
	gridCfg.check()
	tree, err := balancing.NewTree[
		*transport.Connection,
		transport.Connection,
	](treeCfg)
	if err != nil {
		return nil, errors.Join(ErrCreate, err)
	}

	return &Grid{
		Tree:             tree,
		connsPerEndpoint: gridCfg.ConnectionsPerEndpoint,
	}, nil
}

func (g *Grid) AddEndpoint(path []string, connFunc func() (*transport.Connection, error)) error {
	return g.AddPath(balancing.Path[*transport.Connection, transport.Connection]{
		IDs: path,
		ConnectionConfig: balancing.ConnectionConfig[*transport.Connection, transport.Connection]{
			ConnFunc:   connFunc,
			ConnNumber: g.connsPerEndpoint,
		},
	})
}

func (g *Grid) DeleteEndpoint(path []string) error {
	return g.DeletePath(balancing.Path[*transport.Connection, transport.Connection]{
		IDs: path,
	})
}

func (g *Grid) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	trPtr := xcontext.GetTransportPtr(ctx)
	if conn := g.GetConn(); conn != nil {
		if trPtr != nil {
			*trPtr = conn
		}

		return conn.Invoke(ctx, method, args, reply, opts...)
	}

	return errors.Join(localErrs.LocalFailureError{}, ErrNoConnections)
}

func (g *Grid) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if conn := g.GetConn(); conn != nil {
		return conn.NewStream(ctx, desc, method, opts...)
	}

	return nil, errors.Join(localErrs.LocalFailureError{}, ErrNoConnections)
}
