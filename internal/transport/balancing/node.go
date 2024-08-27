package balancing

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/adwski/ydb-go-query/v1/internal/transport/balancing/policy"
)

const (
	defaultConnectionsNumber = 2
	defaultEgressesPrealloc  = 32
)

var (
	ErrConnectionCreate = errors.New("connection create failed")
	ErrNodeClosed       = errors.New("node is closed")
)

type (
	balancingPolicy[PT policy.Egress[T], T any] interface {
		Get([]PT) PT
	}

	connection[T any] interface {
		*T

		policy.Egress[T]

		Close() error
	}

	connFunc[PT connection[T], T any] func() (PT, error)

	node[PT connection[T], T any] struct {
		id string

		// intermediate levels balancing
		egresses     []*node[PT, T]
		egressPolicy balancingPolicy[*node[PT, T], node[PT, T]]

		// connection level balancing
		// TODO: may be egressPolicy and connPolicy can be unified?
		conns      []PT
		connFunc   connFunc[PT, T]
		connPolicy balancingPolicy[PT, T]

		mx     *sync.RWMutex
		closed atomic.Bool
	}

	nodeConfig[PT connection[T], T any] struct {
		ID               string
		Policy           string
		ConnectionConfig *ConnectionConfig[PT, T]
	}
)

func newNode[PT connection[T], T any](cfg nodeConfig[PT, T]) (*node[PT, T], error) {
	if err := validatePolicyKind(cfg.Policy); err != nil {
		return nil, err
	}
	nNode := node[PT, T]{
		id: cfg.ID,
		mx: &sync.RWMutex{},
	}

	if cfg.ConnectionConfig == nil {
		// not leaf
		nNode.egresses = make([]*node[PT, T], 0, defaultEgressesPrealloc)
		nNode.egressPolicy, _ = newPolicy[*node[PT, T], node[PT, T]](cfg.Policy)
		return &nNode, nil
	}

	connCfg := cfg.ConnectionConfig
	nNode.connPolicy, _ = newPolicy[PT, T](cfg.Policy)

	if connCfg.ConnNumber < 1 {
		connCfg.ConnNumber = defaultConnectionsNumber
	}

	for i := 0; i < connCfg.ConnNumber; i++ {
		conn, err := connCfg.ConnFunc()
		if err != nil {
			for _, conn_ := range nNode.conns {
				_ = conn_.Close()
			}
			return nil, errors.Join(ErrConnectionCreate, err)
		}
		nNode.conns = append(nNode.conns, conn)
	}

	return &nNode, nil
}

func (n *node[PT, T]) Alive() bool {
	return true
}

func (n *node[PT, T]) Close() error {
	n.mx.Lock()
	defer n.mx.Unlock()

	n.closed.Store(true)

	for _, egress := range n.egresses {
		_ = egress.Close()
	}
	for _, conn := range n.conns {
		_ = conn.Close()
	}
	n.egresses = nil
	n.conns = nil
	n.connFunc = nil
	n.connPolicy = nil

	return nil
}

func (n *node[PT, T]) lookup(id string) (int, *node[PT, T]) {
	for i, chNode := range n.egresses {
		if chNode.id == id {
			return i, chNode
		}
	}
	return -1, nil
}

func (n *node[PT, T]) detach(idx int) {
	n.mx.Lock()
	defer n.mx.Unlock()

	last := len(n.egresses) - 1
	n.egresses[idx], n.egresses[last] = n.egresses[last], n.egresses[idx]
	n.egresses[last] = nil
	n.egresses = n.egresses[:last]

	return
}

func (n *node[PT, T]) addEgress(e *node[PT, T]) error {
	if n.closed.Load() {
		return ErrNodeClosed
	}
	n.egresses = append(n.egresses, e)
	return nil
}

func (n *node[PT, T]) getBalanced() PT {
	if n.closed.Load() {
		return nil
	}

	if len(n.egresses) > 0 {
		if len(n.egresses) == 1 {
			if n.egresses[0].Alive() {
				return n.egresses[0].getBalanced()
			}
			return nil
		}
		return n.egressPolicy.Get(n.egresses).getBalanced()
	}

	if len(n.conns) > 0 {
		if len(n.conns) == 1 {
			if n.conns[0].Alive() {
				return n.conns[0]
			}
			return nil
		}
		return n.connPolicy.Get(n.conns)
	}

	return nil
}

func (n *node[PT, T]) getBalancedPath() PT {
	if n.closed.Load() {
		return nil
	}

	if len(n.egresses) > 0 {
		if len(n.egresses) == 1 {
			if n.egresses[0].Alive() {
				return n.egresses[0].getBalanced()
			}
			return nil
		}
		return n.egressPolicy.Get(n.egresses).getBalanced()
	}

	if len(n.conns) > 0 {
		if len(n.conns) == 1 {
			if n.conns[0].Alive() {
				return n.conns[0]
			}
			return nil
		}
		return n.connPolicy.Get(n.conns)
	}

	return nil
}
