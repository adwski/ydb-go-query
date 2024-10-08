package v2

import (
	"errors"
	"strconv"
)

const (
	levelKindConnection = "Connection"
	levelKindEndpoint   = "Endpoint"
	levelKindLocation   = "Location"
)

var (
	ErrLevelsEmpty      = errors.New("empty levels")
	ErrPathLen          = errors.New("path length is not equal to levels length")
	ErrPathExists       = errors.New("full path exists")
	ErrPathDoesNotExist = errors.New("path does not exist")
)

type (
	tree[PT connection[T], T any] struct {
		node      *node[PT, T]
		levels    []Level
		levelsNum int
	}

	connectionConfig[PT connection[T], T any] struct {
		ConnFunc   connFunc[PT, T]
		ConnNumber int
	}

	treeConfig[PT connection[T], T any] struct {
		levels []Level
	}

	path[PT connection[T], T any] struct {
		connectionConfig[PT, T]

		ids []string
	}
)

func (l Level) isConnection() bool {
	return l.Kind == levelKindConnection
}

func newTree[PT connection[T], T any](cfg treeConfig[PT, T]) (*tree[PT, T], error) {
	if len(cfg.levels) == 0 {
		return nil, ErrLevelsEmpty
	}

	lvl := cfg.levels[0]

	nodeCfg := nodeConfig[PT, T]{
		id:     lvl.Kind,
		policy: lvl.Policy,
	}

	root, err := newNode(nodeCfg)
	if err != nil {
		return nil, err
	}

	return &tree[PT, T]{
		node:      root,
		levels:    cfg.levels,
		levelsNum: len(cfg.levels),
	}, nil
}

func (t *tree[PT, T]) validatePath(path path[PT, T]) error {
	if len(t.levels)-1 != len(path.ids) {
		return ErrPathLen
	}
	return nil
}

func (t *tree[PT, T]) addPath(path path[PT, T]) error {
	if err := t.validatePath(path); err != nil {
		return err
	}

	var (
		nNode    = t.node
		nextNode *node[PT, T]
		err      error
		idx      int
	)

	// traverse existing nodes
	for ; idx < len(path.ids); idx++ {
		nodeID := path.ids[idx]
		_, nextNode = nNode.lookup(nodeID)
		if nextNode == nil {
			break
		}
		nNode = nextNode
	}

	if idx == len(path.ids) {
		// full path already exists
		return ErrPathExists
	}

	// store current node pointer
	// and create new branch
	var (
		root      = nNode
		newBranch *node[PT, T]
	)

	// add new nodes from the path
	for ; idx < len(path.ids); idx++ {
		// create new node
		nextNode, err = newNode[PT, T](nodeConfig[PT, T]{
			id:     path.ids[idx],
			policy: t.levels[idx].Policy,
		})
		if err != nil {
			return err
		}

		// create new branch or attach new node to it
		if newBranch == nil {
			newBranch = nextNode
		} else {
			nNode.addEgress(nextNode)
		}

		if t.levels[idx+1].isConnection() {
			// create fixed amount of connection level nodes
			if err = addConnectionLevel(nextNode, path.ConnFunc, t.levels[idx+1].Policy, path.ConnNumber); err != nil {
				return err
			}
		} else {
			nNode = nextNode
		}
	}

	if newBranch != nil {
		// attach new branch
		root.addEgress(newBranch)
	}

	return nil
}

func addConnectionLevel[PT connection[T], T any](
	nNode *node[PT, T],
	connF connFunc[PT, T],
	policy string,
	connNumber int,
) error {
	for i := 0; i < connNumber; i++ {
		connNode, errConn := newNode[PT, T](nodeConfig[PT, T]{
			policy:   policy,
			id:       strconv.Itoa(i),
			connFunc: connF,
		})
		if errConn != nil {
			return errConn
		}
		nNode.addEgress(connNode)
	}

	return nil
}

func (t *tree[PT, T]) deletePath(path path[PT, T]) error {
	if err := t.validatePath(path); err != nil {
		return err
	}

	var (
		nNode    = t.node
		delRoot  *node[PT, T]
		prev     *node[PT, T]
		nextNode *node[PT, T]
		idx      int
		prevIdx  int
		delIdx   int
	)

	for i, nodeID := range path.ids {
		if len(nNode.egresses) == 1 {
			delRoot = prev
			delIdx = prevIdx
		}

		idx, nextNode = nNode.lookup(nodeID)
		if nextNode == nil {
			// path to not-existing node
			return ErrPathDoesNotExist
		}
		if i == len(path.ids)-1 {
			if delRoot != nil {
				delRoot.detach(delIdx)
			} else {
				nNode.detach(idx)
			}

			break
		}

		nNode, prev = nextNode, nNode
		prevIdx = idx
	}

	return nNode.Close()
}
