package v3

import (
	"errors"
	"sync"
)

const (
	defaultLocation = "&&def"

	minConnectionsPerEndpoint = 1
)

var (
	ErrConnCreate = errors.New("connection create failed")

	ErrUnknownLocation = errors.New("unknown location")
	ErrNoSuchID        = errors.New("no such id")
	ErrEmptyLocation   = errors.New("empty location")
)

type (
	connection[T any] interface {
		*T

		Alive() bool
		Close() error
		ID() uint64
	}

	node[PT connection[T], T any] struct {
		next *node[PT, T]
		conn PT
	}

	locationData[PT connection[T], T any] struct {
		lookupPtr *node[PT, T] // points to next balancing decision
		insertPtr *node[PT, T] // points to insertion point for new connections
		size      int          // amount of connections inside current location
	}

	// Grid is fixed-level load balancer that is specifically
	// balances between connections grouped by locations.
	//
	Grid[PT connection[T], T any] struct {
		mx *sync.Mutex

		locDta map[string]locationData[PT, T] // connections per location

		locPrefM map[string]struct{} // locations with configured preference
		locPref  []string            // ordered preference for locations

		connsPerEndpoint int  // amount of connsPerEndpoint to spawn for each endpoint
		ignoreLocations  bool // use default locations for all endpoints
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
	if len(cfg.LocationPreference) == 0 {
		cfg.IgnoreLocations = true
	}
	if cfg.ConnsPerEndpoint < 1 {
		cfg.ConnsPerEndpoint = minConnectionsPerEndpoint
	}

	grid := &Grid[PT, T]{
		mx: &sync.Mutex{},

		locDta:   make(map[string]locationData[PT, T]),
		locPrefM: make(map[string]struct{}),

		locPref:          cfg.LocationPreference,
		connsPerEndpoint: cfg.ConnsPerEndpoint,
		ignoreLocations:  cfg.IgnoreLocations,
	}

	for _, location := range cfg.LocationPreference {
		grid.locPrefM[location] = struct{}{}
	}

	return grid
}

// GetConn selects balanced connection based on available
// locations and alive connections.
// - It will always return connections from first location
// in LocationPreference list.
// - If there's no alive connections in first location,
// next location from the list is used (and so on).
// - If there's no alive connections in any of preferred locations,
// other existing location will be checked in no particular order.
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
	for loc := range g.locDta {
		if _, ok := g.locPrefM[loc]; !ok {
			conn := g.lookupInLocation(loc)
			if conn != nil {
				return conn
			}
		}
	}

	return nil
}

func (g *Grid[PT, T]) lookupInLocation(location string) PT {
	var (
		loc  = g.locDta[location]
		ptr  = loc.lookupPtr
		size = loc.size
	)

	// Get next alive conn,
	// making full circle in worst case.
	for ; size > 0; size-- {
		if ptr.conn.Alive() {
			loc.lookupPtr = ptr.next
			g.locDta[location] = loc

			return ptr.conn
		}
		ptr = ptr.next
	}

	return nil
}

// Add creates connections in specified location.
func (g *Grid[PT, T]) Add(location string, creatF createFunc[PT, T]) error {
	// Spawn connections
	var (
		head, prev *node[PT, T]
	)
	for range g.connsPerEndpoint {
		conn, err := creatF()
		if err != nil {
			for ; head != nil; head = head.next {
				_ = head.conn.Close()
			}
			return errors.Join(ErrConnCreate, err)
		}

		if prev == nil {
			prev = &node[PT, T]{conn: conn}
			head = prev
		} else {
			prev.next = &node[PT, T]{conn: conn}
			prev = prev.next
		}
	}

	// Attach connections to location
	g.mx.Lock()
	defer g.mx.Unlock()

	if g.ignoreLocations {
		location = defaultLocation
	}
	locDta, ok := g.locDta[location]
	if ok && locDta.size != 0 {
		// insert conn list into location list
		locDta.insertPtr.next, prev.next = head, locDta.insertPtr.next
		locDta.size += g.connsPerEndpoint
		g.locDta[location] = locDta

		return nil
	}

	// First time seeing this location
	prev.next = head // cycle nodes
	g.locDta[location] = locationData[PT, T]{
		lookupPtr: head,
		insertPtr: prev,
		size:      g.connsPerEndpoint,
	}

	return nil
}

// Delete deletes connections from location.
// It uses linear search within location to find all matching connections.
func (g *Grid[PT, T]) Delete(location string, id uint64) error {
	g.mx.Lock()
	defer g.mx.Unlock()

	if g.ignoreLocations {
		location = defaultLocation
	}
	locDta, ok := g.locDta[location]
	switch {
	case !ok:
		return ErrUnknownLocation
	case locDta.size == 0:
		return ErrEmptyLocation
	case locDta.size == g.connsPerEndpoint:
		// We have connsPerEndpoint of only one endpoint.
		if locDta.insertPtr.conn.ID() == id {
			// delete last remaining endpoint
			delete(g.locDta, location)

			return nil
		}

		return ErrNoSuchID
	}

	var (
		start *node[PT, T]

		// prev and ptr are starting at border between some endpoints
		ptr  = locDta.insertPtr.next
		prev = locDta.insertPtr
	)

	// New conns for the same endpoint are always added
	// as continuous range. To delete conns by endpoint id
	// we need to find conn (start) preceding first conn of this endpoint
	// then find first conn of next endpoint.
	// And finally point start.next to conn of next endpoint.
	for size := locDta.size; size >= 0; size-- {
		if ptr.conn.ID() == id {
			// found first conn in deletion range
			start = prev

			// scroll to conn of next endpoint
			for ctr := 0; ctr < g.connsPerEndpoint; ctr++ {
				ptr = ptr.next
			}
			// found last conn
			start.next = ptr

			// Warp lookup and insert pointers
			// if they are in deleted range.
			if locDta.insertPtr.conn.ID() == id {
				locDta.insertPtr = ptr
			}
			if locDta.lookupPtr.conn.ID() == id {
				locDta.lookupPtr = ptr
			}

			g.locDta[location] = locDta

			return nil
		}

		prev, ptr = ptr, ptr.next
	}

	return ErrNoSuchID
}
