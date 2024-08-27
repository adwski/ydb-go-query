package discovery

import (
	"sync"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
)

type (
	// EndpointInfoShort uniquely identifies YDB endpoint.
	EndpointInfoShort struct {
		Address  string
		Location string
		NodeID   uint32
		Port     uint32
	}

	EndpointMap map[EndpointInfoShort]*Ydb_Discovery.EndpointInfo

	EndpointDB struct {
		mx *sync.RWMutex
		db EndpointMap
	}
)

func (eis EndpointInfoShort) GetAddress() string {
	return eis.Address
}

func (eis EndpointInfoShort) GetPort() uint32 {
	return eis.Port
}

func NewEndpointDB() EndpointDB {
	return EndpointDB{
		mx: &sync.RWMutex{},
		db: make(EndpointMap),
	}
}

func (epDB *EndpointDB) getAll() EndpointMap {
	epDB.mx.RLock()
	defer epDB.mx.RUnlock()

	eps := make(EndpointMap, len(epDB.db))
	for k, v := range epDB.db {
		eps[k] = v
	}
	return eps
}

func (epDB *EndpointDB) update(endpoints []*Ydb_Discovery.EndpointInfo) (Announce, int, int) {
	oldDB := epDB.getAll()
	newDB := make(EndpointMap, len(endpoints))

	prev := len(oldDB)
	length := len(endpoints)

	ann := Announce{
		Add: make(EndpointMap, length),
		// Update: make(EndpointMap, length), // TODO
		Del: make([]EndpointInfoShort, 0, length),
	}

	for _, ep := range endpoints {
		key := EndpointInfoShort{
			NodeID:   ep.NodeId,
			Location: ep.Location,
			Address:  ep.Address,
			Port:     ep.Port,
		}
		if _, ok := epDB.db[key]; !ok {
			ann.Add[key] = ep
		}
		newDB[key] = ep
	}

	for k := range oldDB {
		if _, ok := newDB[k]; !ok {
			ann.Del = append(ann.Del, k)
		}
	}

	epDB.swap(newDB)

	return ann, prev, length
}

func (epDB *EndpointDB) swap(db EndpointMap) {
	epDB.mx.Lock()
	defer epDB.mx.Unlock()

	epDB.db = db
}

func (epDB *EndpointDB) compare(endpoints []*Ydb_Discovery.EndpointInfo) bool {
	epDB.mx.RLock()
	defer epDB.mx.RUnlock()

	ctr := len(epDB.db)
	for _, ep := range endpoints {
		if _, ok := epDB.db[EndpointInfoShort{
			NodeID:   ep.NodeId,
			Location: ep.Location,
			Address:  ep.Address,
			Port:     ep.Port,
		}]; !ok {
			return false
		}
		ctr--
	}

	return ctr == 0
}
