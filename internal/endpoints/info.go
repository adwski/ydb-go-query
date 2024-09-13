package endpoints

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"

type (
	// InfoShort uniquely identifies YDB endpoint.
	InfoShort struct {
		Address  string
		Location string
		NodeID   uint32
		Port     uint32
	}
)

func NewInfoShort(ep *Ydb_Discovery.EndpointInfo) InfoShort {
	return InfoShort{
		NodeID:   ep.NodeId,
		Location: ep.Location,
		Address:  ep.Address,
		Port:     ep.Port,
	}
}

func (eis *InfoShort) GetAddress() string {
	return eis.Address
}

func (eis *InfoShort) GetPort() uint32 {
	return eis.Port
}
