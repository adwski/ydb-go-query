package v2

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	defaultConnectionsPerEndpoint = 2
)

var (
	ErrCreate       = errors.New("unable to initialize grid")
	ErrGridEndpoint = errors.New("error adding static endpoint")
)

type (
	Grid[PT connection[T], T any] struct {
		*tree[PT, T]

		getCh chan PT
		addCh chan path[PT, T]
		delCh chan path[PT, T]

		connsPerEndpoint int
	}

	Config[PT connection[T], T any] struct {
		treeCfg treeConfig[PT, T]

		ConnectionsPerEndpoint int
	}

	Level struct {
		Kind   string
		Policy string
	}
)

func (c *Config[PT, T]) check() {
	if c.ConnectionsPerEndpoint < 1 {
		c.ConnectionsPerEndpoint = defaultConnectionsPerEndpoint
	}
}

func NewWithStaticEndpoints[PT connection[T], T any](
	ctx context.Context,
	endpoints []string,
	connFunc func(string) (PT, error),
) (*Grid[PT, T], error) {
	tr := NewWithTwoLevels(Config[PT, T]{
		ConnectionsPerEndpoint: 1,
	})

	for _, addr := range endpoints {
		err := tr.addPath(path[PT, T]{
			ids: []string{addr},
			connectionConfig: connectionConfig[PT, T]{
				ConnFunc: func() (PT, error) {
					return connFunc(addr)
				},
				ConnNumber: 1,
			},
		})

		if err != nil {
			return nil, errors.Join(ErrGridEndpoint, err)
		}
	}

	return tr, nil
}

func NewWithTwoLevels[PT connection[T], T any](cfg Config[PT, T]) *Grid[PT, T] {
	cfg.check()
	cfg.treeCfg = treeConfig[PT, T]{
		levels: []Level{
			{
				Kind:   levelKindEndpoint,
				Policy: PolicyKindRoundRobin,
			},
			{
				Kind:   levelKindConnection,
				Policy: PolicyKindRoundRobin,
			},
		},
	}
	grid, _ := newGrid(cfg)

	return grid
}

func NewWithThreeLevels[PT connection[T], T any](cfg Config[PT, T]) *Grid[PT, T] {
	cfg.check()
	cfg.treeCfg = treeConfig[PT, T]{
		levels: []Level{
			{
				Kind:   levelKindLocation,
				Policy: PolicyKindFirstReady,
			},
			{
				Kind:   levelKindEndpoint,
				Policy: PolicyKindRoundRobin,
			},
			{
				Kind:   levelKindConnection,
				Policy: PolicyKindRoundRobin,
			},
		},
	}
	grid, _ := newGrid(cfg)

	return grid
}

func newGrid[PT connection[T], T any](cfg Config[PT, T]) (*Grid[PT, T], error) {
	cfg.check()
	tr, err := newTree[PT, T](cfg.treeCfg)
	if err != nil {
		return nil, errors.Join(ErrCreate, err)
	}

	return &Grid[PT, T]{
		tree: tr,

		addCh: make(chan path[PT, T]),
		delCh: make(chan path[PT, T]),
		getCh: make(chan PT),

		connsPerEndpoint: cfg.ConnectionsPerEndpoint,
	}, nil
}

func (g *Grid[PT, T]) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	timer := time.NewTimer(0)
	defer timer.Stop()

	next := g.tree.node.getBalanced()
runLoop:
	for {
		select {
		case <-ctx.Done():
			break runLoop
		case pth := <-g.addCh:
			_ = g.addPath(pth)
			if next == nil {
				next = g.tree.node.getBalanced()
			}
		case pth := <-g.delCh:
			_ = g.deletePath(pth)
		case g.getCh <- next:
			next = g.tree.node.getBalanced()
		}
	}
}

func (g *Grid[PT, T]) GetBalanced() PT {
	return <-g.getCh
}

func (g *Grid[PT, T]) AddEndpoint(pth []string, connFunc func() (PT, error)) error {
	g.addCh <- path[PT, T]{
		ids: pth,
		connectionConfig: connectionConfig[PT, T]{
			ConnFunc:   connFunc,
			ConnNumber: g.connsPerEndpoint,
		},
	}
	return nil
}

func (g *Grid[PT, T]) DeleteEndpoint(pth []string) error {
	g.delCh <- path[PT, T]{
		ids: pth,
	}

	return nil
}

//nolint:godot // just saved
/*
func (g *Grid[PT, T]) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	trPtr := xcontext.GetTransportPtr(ctx)
	select {
	case <-ctx.Done():
		return errors.Join(localErrs.LocalFailureError{}, ctx.Err())
	case conn := <-g.getCh:
		if conn == nil {
			return errors.Join(localErrs.LocalFailureError{}, ErrNoConnections)
		}
		if trPtr != nil {
			*trPtr = conn
		}

		return conn.Invoke(ctx, method, args, reply, opts...) //nolint:wrapcheck // unnecessary
	}
}

func (g *Grid) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Join(localErrs.LocalFailureError{}, ctx.Err())
	case conn := <-g.getCh:
		if conn == nil {
			return nil, errors.Join(localErrs.LocalFailureError{}, ErrNoConnections)
		}

		return conn.NewStream(ctx, desc, method, opts...) //nolint:wrapcheck // unnecessary
	}
}
*/
