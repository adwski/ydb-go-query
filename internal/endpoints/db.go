package endpoints

import (
	"sync"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
)

type (
	// Announce is helpful message for consumers of this service about changes in YDB endpoints.
	// For example dispatcher uses it to adjust balancing tree.
	Announce struct {
		Add    Map         // contains newly discovered endpoints
		Update Map         // contains endpoints with changes (reserved for later use with load factor)
		Del    []InfoShort // contains endpoints that are no longer present in YDB cluster
	}

	// Map stores endpoints as kay-value structure.
	Map map[InfoShort]*Ydb_Discovery.EndpointInfo

	// DB is thread safe in-memory storage for endpoints.
	DB struct {
		mx  *sync.RWMutex
		dbm Map
	}
)

// NewDB creates endpoints DB.
func NewDB() DB {
	return DB{
		mx:  &sync.RWMutex{},
		dbm: make(Map),
	}
}

// GetAll returns copy of internal endpoints Map.
func (db *DB) GetAll() Map {
	db.mx.RLock()
	defer db.mx.RUnlock()

	eps := make(Map, len(db.dbm))
	for k, v := range db.dbm {
		eps[k] = v
	}
	return eps
}

// Compare takes current state of endpoints and compares it
// with internal endpoints Map. It returns true if incoming state
// is identical to internal or false otherwise.
func (db *DB) Compare(endpoints []*Ydb_Discovery.EndpointInfo) bool {
	db.mx.RLock()
	defer db.mx.RUnlock()

	ctr := len(db.dbm)
	for _, ep := range endpoints {
		if _, ok := db.dbm[NewInfoShort(ep)]; !ok {
			return false
		}
		ctr--
	}

	return ctr == 0
}

// Update takes current state of endpoints and
// - updates internal DB accordingly
// - constructs endpoints announcement that reflects performed changes.
func (db *DB) Update(endpoints []*Ydb_Discovery.EndpointInfo) (Announce, int, int) {
	oldDB := db.GetAll()
	newDB := make(Map, len(endpoints))

	prev := len(oldDB)
	length := len(endpoints)

	ann := Announce{
		Add: make(Map, length),
		// Update: make(Map, length), // TODO
		Del: make([]InfoShort, 0, length),
	}

	for _, ep := range endpoints {
		key := InfoShort{
			NodeID:   ep.NodeId,
			Location: ep.Location,
			Address:  ep.Address,
			Port:     ep.Port,
		}
		if _, ok := db.dbm[key]; !ok {
			ann.Add[key] = ep
		}
		newDB[key] = ep
	}

	for k := range oldDB {
		if _, ok := newDB[k]; !ok {
			ann.Del = append(ann.Del, k)
		}
	}

	db.swap(newDB)

	return ann, prev, length
}

func (db *DB) swap(dbm Map) {
	db.mx.Lock()
	defer db.mx.Unlock()

	db.dbm = dbm
}
