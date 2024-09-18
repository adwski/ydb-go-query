package endpoints

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
)

const (
	serviceNameQuery = "query_service"
)

type (
	Filter struct {
		Require *Require
		Prefer  *Prefer
	}

	Require struct {
		Services  []string
		Locations []string
	}

	Prefer struct {
		Locations []string
	}
)

func NewFilter() *Filter {
	return &Filter{
		Require: &Require{},
	}
}

func (f *Filter) WithQueryService() *Filter {
	f.Require.Services = append(f.Require.Services, serviceNameQuery)

	return f
}

func (f *Filter) matchRequired(ep *Ydb_Discovery.EndpointInfo) bool {
	if f.Require == nil {
		return true
	}

	if !matchServices(ep, f.Require.Services) {
		return false
	}

	return matchLocation(ep, f.Require.Locations)
}

func (f *Filter) matchPreferred(ep *Ydb_Discovery.EndpointInfo) bool {
	if f.Prefer == nil {
		return true
	}

	return matchLocation(ep, f.Prefer.Locations)
}

func (f *Filter) Filter(endpoints []*Ydb_Discovery.EndpointInfo) (
	preferred []*Ydb_Discovery.EndpointInfo,
	notPreferred []*Ydb_Discovery.EndpointInfo,
) {
	for _, ep := range endpoints {
		if f.matchRequired(ep) {
			if f.matchPreferred(ep) {
				preferred = append(preferred, ep)
			} else {
				notPreferred = append(notPreferred, ep)
			}
		}
	}

	return
}

func matchServices(ep *Ydb_Discovery.EndpointInfo, services []string) bool {
	srvs := make(map[string]struct{})
	for _, srv := range ep.Service {
		srvs[srv] = struct{}{}
	}
	for _, srv := range services {
		if _, ok := srvs[srv]; !ok {
			return false
		}
	}

	return true
}

func matchLocation(ep *Ydb_Discovery.EndpointInfo, locations []string) bool {
	if len(locations) == 0 {
		return true
	}

	matchLoc := false
	for _, loc := range locations {
		if loc == ep.Location {
			matchLoc = true
		}
	}

	return matchLoc
}
