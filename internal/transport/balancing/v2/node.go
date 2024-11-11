package v2

import (
	"errors"

	"github.com/adwski/ydb-go-query/internal/transport/balancing/v2/policy"
)

const (
	defaultEgressesPrealloc = 32
)

var (
	ErrConnectionCreate = errors.New("connection create failed")
)

type (
	balancingPolicy[PT policy.Egress[T], T any] interface {
		Get([]PT, []int) PT
	}

	connection[T any] interface {
		*T

		policy.Egress[T]

		Close() error
	}

	connFunc[PT connection[T], T any] func() (PT, error)

	node[PT connection[T], T any] struct {
		id string

		// connection
		conn PT

		// intermediate levels
		policy   balancingPolicy[*node[PT, T], node[PT, T]]
		egresses []*node[PT, T]
		weights  []int
	}

	nodeConfig[PT connection[T], T any] struct {
		connFunc connFunc[PT, T]
		id       string
		policy   string
	}
)

func newNode[PT connection[T], T any](cfg nodeConfig[PT, T]) (*node[PT, T], error) {
	if err := validatePolicyKind(cfg.policy); err != nil {
		return nil, err
	}
	nNode := node[PT, T]{
		id: cfg.id,
	}

	if cfg.connFunc == nil {
		// not leaf
		nNode.egresses = make([]*node[PT, T], 0, defaultEgressesPrealloc)
		nNode.policy, _ = newPolicy[*node[PT, T], node[PT, T]](cfg.policy)

		return &nNode, nil
	}

	// leaf (node just wraps connection)
	conn, err := cfg.connFunc()
	if err != nil {
		return nil, errors.Join(ErrConnectionCreate, err)
	}

	nNode.conn = conn

	return &nNode, nil
}

func (n *node[PT, T]) Alive() bool {
	return true
}

func (n *node[PT, T]) Close() error {
	for _, egress := range n.egresses {
		_ = egress.Close()
	}

	_ = n.conn.Close()
	n.egresses = nil

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
	last := len(n.egresses) - 1
	n.egresses[idx], n.egresses[last] = n.egresses[last], n.egresses[idx]
	n.egresses[last] = nil
	n.egresses = n.egresses[:last]
}

func (n *node[PT, T]) addEgress(e *node[PT, T]) {
	n.egresses = append(n.egresses, e)
}

func (n *node[PT, T]) getBalanced() PT {
	if len(n.egresses) > 0 {
		if len(n.egresses) == 1 {
			return n.egresses[0].getBalanced()
		}

		return n.policy.Get(n.egresses, n.weights).getBalanced()
	}

	if n.conn != nil && n.conn.Alive() {
		return n.conn
	}

	return nil
}
