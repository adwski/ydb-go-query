package v3

import (
	"errors"
	"sync"
)

const (
	nodesPrealloc = 16

	defaultLocation = "&&def"
)

var (
	ErrConnCreate = errors.New("connection create failed")

	ErrUnknownLocation = errors.New("unknown location")
	ErrNoSuchID        = errors.New("no such id")
)

type (
	connection[T any] interface {
		*T

		Alive() bool
		Close() error
		ID() uint64
	}

	// Grid is fixed-level load balancer that is specifically
	// balances between connections grouped by locations.
	//
	Grid[PT connection[T], T any] struct {
		mx *sync.Mutex

		locDta   map[string][]PT // connections per location
		connIdxs map[string]int  // next available indexes per location
		blnIdxs  map[string]int  // round-robin indexes per location

		locPrefM map[string]struct{} // locations with configured preference
		locPref  []string            // ordered preference for locations

		conns           int  // amount of conns to spawn for each endpoint
		ignoreLocations bool // use default locations for all endpoints
	}

	// Config provides initial params for Grid.
	Config struct {
		// LocationPreference defines location processing sequence.
		// See GetConn().
		LocationPreference []string

		// ConnsPerEndpoint specifies how many individual connections will be
		// spawned during Add() call.
		ConnsPerEndpoint int

		// IgnoreLocations explicitly sets to ignore LocationPreference and
		// use common default location for all endpoints.
		// If LocationPreference is empty, default location is used regardless
		// of this flag's value.
		IgnoreLocations bool
	}

	createFunc[PT connection[T], T any] func() (PT, error)
)

// NewGrid creates new grid load balancer.
func NewGrid[PT connection[T], T any](cfg Config) *Grid[PT, T] {
	dta := make(map[string][]PT)
	locPrefM := make(map[string]struct{})

	for _, location := range cfg.LocationPreference {
		dta[location] = make([]PT, 0, cfg.ConnsPerEndpoint*nodesPrealloc)
		locPrefM[location] = struct{}{}
	}
	if len(cfg.LocationPreference) == 0 {
		dta[defaultLocation] = make([]PT, 0, cfg.ConnsPerEndpoint*nodesPrealloc)
		cfg.IgnoreLocations = true
	}

	grid := &Grid[PT, T]{
		mx: &sync.Mutex{},

		locDta:   dta,
		locPrefM: locPrefM,
		connIdxs: make(map[string]int),
		blnIdxs:  make(map[string]int),

		locPref:         cfg.LocationPreference,
		conns:           cfg.ConnsPerEndpoint,
		ignoreLocations: cfg.IgnoreLocations,
	}

	return grid
}

// GetConn selects balanced connection based on available
// locations and alive connections.
// - It will always return connections from first location
// in LocationPreference list.
// - If there's no alive connections in current location,
// next location from the list is used (and so on).
// - If end of list is reached and there's still alive endpoints
// from locations that are not in this list, GetConn() will select
// them in no particular location-order.
// - If IgnoreLocations is set, it uses only default location.
//
// Within one location connections are selected using round-robin approach.
func (g *Grid[PT, T]) GetConn() PT {
	g.mx.Lock()
	defer g.mx.Unlock()

	if g.ignoreLocations {
		return g.lookupInLocation(defaultLocation)
	}

	// lookup in available locations according to preference
	for _, loc := range g.locPref {
		if _, ok := g.locDta[loc]; ok {
			conn := g.lookupInLocation(loc)
			if conn != nil {
				return conn
			}
		}
	}

	// If some locations are not in preference,
	// lookup inside them as well.
	if len(g.locDta) > len(g.locPref) {
		for loc := range g.locDta {
			if _, ok := g.locPrefM[loc]; !ok {
				conn := g.lookupInLocation(loc)
				if conn != nil {
					return conn
				}
			}
		}
	}

	return nil
}

func (g *Grid[PT, T]) lookupInLocation(location string) PT {
	var (
		idx   = g.blnIdxs[location]
		nodes = g.locDta[location]
		ln    = len(nodes)
	)

	// Get next alive conn,
	// making full circle in worst case.
	for i := idx; i < idx+ln; i++ {
		conn := nodes[i%ln]
		if conn != nil && conn.Alive() {
			g.blnIdxs[location] = (i + 1) % ln
			return conn
		}
	}

	return nil
}

// Add creates connections in specified location.
func (g *Grid[PT, T]) Add(location string, creatF createFunc[PT, T]) error {
	g.mx.Lock()
	defer g.mx.Unlock()

	// Spawn connections
	newConns := make([]PT, 0, g.conns)
	for range g.conns {
		conn, err := creatF()
		if err != nil {
			for _, conn_ := range newConns {
				_ = conn_.Close()
			}
			return errors.Join(ErrConnCreate, err)
		}

		newConns = append(newConns, conn)
	}

	if g.ignoreLocations {
		location = defaultLocation
	}
	nodes, ok := g.locDta[location]

	if !ok {
		// First time seeing this location
		newLoc := make([]PT, g.conns, g.conns*nodesPrealloc)
		copy(newLoc, newConns)
		g.locDta[location] = newLoc
		g.connIdxs[location] = g.conns

		return nil
	}

	// get next available index
	idx := g.connIdxs[location]

	// add new connections to location
	for _, conn := range newConns {
		for {
			if idx == len(nodes) {
				nodes = append(nodes, conn)
				g.locDta[location] = nodes
				idx++
				break
			} else if nodes[idx] == nil {
				nodes[idx] = conn
				idx++
				break
			}
			idx++
		}
	}
	g.connIdxs[location] = idx

	return nil
}

// Delete deletes connections from location.
// It uses linear search within location to find all matching connections.
// 'Slots' from deleted connections can be reused later by Add().
func (g *Grid[PT, T]) Delete(location string, id uint64) error {
	g.mx.Lock()
	defer g.mx.Unlock()

	if g.ignoreLocations {
		location = defaultLocation
	}
	nodes, ok := g.locDta[location]
	if !ok {
		return ErrUnknownLocation
	}

	deleted := false

	// Search for connections with given ID and delete them from location.
	for idx, nd := range nodes {
		if nd != nil && nd.ID() == id {
			if idx < g.connIdxs[location] {
				// update next available index
				g.connIdxs[location] = idx
			}
			deleted = true
			_ = nd.Close()
			nodes[idx] = nil
		}
	}

	if !deleted {
		return ErrNoSuchID
	}

	return nil
}
