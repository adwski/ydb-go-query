package balancing

import "errors"

const (
	LevelKindConnection = "Connection"
	LevelKindEndpoint   = "Endpoint"
	LevelKindLocation   = "Location"
)

var (
	ErrLevelsEmpty               = errors.New("empty levels")
	ErrPathLen                   = errors.New("path length is not equal to levels length")
	ErrConnectionConfigMisplaced = errors.New("connection config must be provided only for connection level")
)

type (
	Level struct {
		Kind   string
		Policy string
	}

	Tree[PT connection[T], T any] struct {
		node      *node[PT, T]
		levels    []Level
		levelsNum int
	}

	TreeConfig[PT connection[T], T any] struct {
		Levels           []Level
		ConnectionConfig *ConnectionConfig[PT, T]
	}

	ConnectionConfig[PT connection[T], T any] struct {
		ConnFunc   connFunc[PT, T]
		ConnNumber int
	}

	Path[PT connection[T], T any] struct {
		IDs []string

		ConnectionConfig[PT, T]
	}
)

func (l Level) IsConnection() bool {
	return l.Kind == LevelKindConnection
}

func NewTree[PT connection[T], T any](cfg TreeConfig[PT, T]) (*Tree[PT, T], error) {
	if len(cfg.Levels) == 0 {
		return nil, ErrLevelsEmpty
	}

	lvl := cfg.Levels[0]

	nodeCfg := nodeConfig[PT, T]{
		ID:     lvl.Kind,
		Policy: lvl.Policy,
	}

	if lvl.IsConnection() {
		if cfg.ConnectionConfig == nil {
			return nil, ErrConnectionConfigMisplaced
		}
		nodeCfg.ConnectionConfig = cfg.ConnectionConfig
	} else if cfg.ConnectionConfig != nil {
		return nil, ErrConnectionConfigMisplaced
	}

	root, err := newNode(nodeCfg)
	if err != nil {
		return nil, err
	}

	return &Tree[PT, T]{
		node:      root,
		levels:    cfg.Levels,
		levelsNum: len(cfg.Levels),
	}, nil
}

func (t *Tree[PT, T]) validatePath(path Path[PT, T]) error {
	if len(t.levels)-1 != len(path.IDs) {
		return ErrPathLen
	}
	return nil
}

func (t *Tree[PT, T]) connectionConfigForLevel(lvlIdx int, connCfg *ConnectionConfig[PT, T]) *ConnectionConfig[PT, T] {
	if t.levels[lvlIdx].IsConnection() {
		return connCfg
	}
	return nil
}

func (t *Tree[PT, T]) GetConn() PT {
	return t.node.getBalanced()
}

func (t *Tree[PT, T]) AddPath(path Path[PT, T]) error {
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
	for ; idx < len(path.IDs); idx++ {
		nodeID := path.IDs[idx]
		_, nextNode = nNode.lookup(nodeID)
		if nextNode == nil {
			break
		}
		nNode = nextNode
	}

	if idx == len(path.IDs) {
		// full path already exists
		// return silently for now
		return nil
	}

	// store current node pointer
	// and create new branch
	var (
		root      = nNode
		newBranch *node[PT, T]
	)

	for ; idx < len(path.IDs); idx++ {
		if nextNode, err = newNode[PT, T](nodeConfig[PT, T]{
			ID:               path.IDs[idx],
			Policy:           t.levels[idx+1].Policy,
			ConnectionConfig: t.connectionConfigForLevel(idx+1, &path.ConnectionConfig),
		}); err != nil {
			return err
		}
		if newBranch == nil {
			newBranch = nextNode
		} else {
			_ = nNode.addEgress(nextNode) // node is not yet attached and therefore should not be closed
		}
		nNode = nextNode
	}

	if newBranch != nil {
		// attach new branch
		root.mx.Lock()
		err = root.addEgress(newBranch)
		root.mx.Unlock()

		if err != nil {
			_ = newBranch.Close()
			return err
		}
	}

	return nil
}

func (t *Tree[PT, T]) DeletePath(path Path[PT, T]) error {
	if err := t.validatePath(path); err != nil {
		return err
	}

	var (
		nNode    = t.node
		nextNode *node[PT, T]
		idx      int
	)

	for i, nodeID := range path.IDs {
		idx, nextNode = nNode.lookup(nodeID)
		if nextNode == nil {
			// path to not-existing node
			// return silently for now
			return nil
		}
		if i == len(path.IDs)-1 {
			nNode.detach(idx)
		}
		nNode = nextNode
	}

	return nNode.Close()
}
