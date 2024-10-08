package v1

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/adwski/ydb-go-query/internal/transport/balancing/v1/policy"
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
		mx *sync.RWMutex

		id string

		// intermediate levels balancing
		egressPolicy balancingPolicy[*node[PT, T], node[PT, T]]
		egresses     []*node[PT, T]

		// connection level balancing
		// TODO: may be egressPolicy and connPolicy can be unified?
		connFunc   connFunc[PT, T]
		connPolicy balancingPolicy[PT, T]
		conns      []PT

		alive  atomic.Bool
		closed bool
	}

	nodeConfig[PT connection[T], T any] struct {
		ConnectionConfig *ConnectionConfig[PT, T]
		ID               string
		Policy           string
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
	nNode.alive.Store(true) // mark alive because node has connections

	return &nNode, nil
}

func (n *node[PT, T]) Alive() bool {
	return n.alive.Load()
}

func (n *node[PT, T]) Close() error {
	n.mx.Lock()
	defer n.mx.Unlock()

	n.closed = true

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

	if len(n.egresses) == 0 && len(n.conns) == 0 {
		n.alive.Store(false)
	}
}

func (n *node[PT, T]) addEgress(e *node[PT, T]) error {
	n.mx.Lock()
	defer n.mx.Unlock()

	if n.closed {
		return ErrNodeClosed
	}
	n.egresses = append(n.egresses, e)
	n.alive.Store(true)

	return nil
}

func (n *node[PT, T]) getBalanced() PT {
	n.mx.RLock()
	defer n.mx.RUnlock()

	if n.closed {
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
