package endpoints

import (
	"hash/maphash"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
)

var (
	hashSeed = maphash.MakeSeed()
)

type (
	// InfoShort uniquely identifies YDB endpoint.
	InfoShort struct {
		Address     string
		Location    string
		AddressHash uint64
		NodeID      uint32
		Port        uint32
	}
)

func NewInfoShort(ep *Ydb_Discovery.EndpointInfo) InfoShort {
	return InfoShort{
		NodeID:   ep.NodeId,
		Location: ep.Location,
		Address:  ep.Address,
		Port:     ep.Port,

		AddressHash: maphash.String(hashSeed, ep.Address),
	}
}

func NewInfoShortFromParams(location, address string, nodeID, port uint32) InfoShort {
	return InfoShort{
		NodeID:   nodeID,
		Location: location,
		Address:  address,
		Port:     port,

		AddressHash: maphash.String(hashSeed, address),
	}
}

func (eis *InfoShort) GetAddress() string {
	return eis.Address
}

func (eis *InfoShort) GetPort() uint32 {
	return eis.Port
}
