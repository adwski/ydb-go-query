package dispatcher

import (
	"context"
	"strconv"
	"sync"

	"github.com/adwski/ydb-go-query/v1/internal/endpoints"
	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/transport"
	"github.com/adwski/ydb-go-query/v1/internal/transport/balancing/grid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type (
	grpcBalancer interface {
		grpc.ClientConnInterface

		AddEndpoint([]string, func() (*transport.Connection, error)) error
		DeleteEndpoint([]string) error
	}

	EndpointsProvider interface {
		EndpointsChan() <-chan endpoints.Announce
	}

	// Dispatcher provides dynamic transport layer by
	// gluing together endpoints provider and balancer.
	//
	// It uses provider's announces to populate balancing grid.
	//
	// Balancing grid in turn is used by YDB services
	// as abstract transport.
	Dispatcher struct {
		logger   logger.Logger
		balancer grpcBalancer

		discovery EndpointsProvider

		transportCredentials credentials.TransportCredentials
		auth                 transport.Authenticator

		db string
	}

	Config struct {
		Logger               logger.Logger
		EndpointsProvider    EndpointsProvider
		Auth                 transport.Authenticator
		TransportCredentials credentials.TransportCredentials
		DB                   string
		InitNodes            []string
		GridConfig           grid.Config
	}
)

func New(cfg Config) *Dispatcher {
	return &Dispatcher{
		logger:               cfg.Logger,
		discovery:            cfg.EndpointsProvider,
		transportCredentials: cfg.TransportCredentials,
		auth:                 cfg.Auth,
		db:                   cfg.DB,
		balancer:             grid.NewWithThreeLevels(cfg.GridConfig),
	}
}

func (r *Dispatcher) Transport() grpc.ClientConnInterface {
	return r.balancer
}

func (r *Dispatcher) Run(ctx context.Context, wg *sync.WaitGroup) {
	r.logger.Debug("dispatcher started")
	defer func() {
		r.logger.Debug("dispatcher stopped")
		wg.Done()
	}()

runLoop:
	for {
		select {
		case <-ctx.Done():
			break runLoop
		case ann := <-r.discovery.EndpointsChan():
			r.processAnnounce(ctx, ann)
		}
	}
}

func (r *Dispatcher) processAnnounce(ctx context.Context, ann endpoints.Announce) {
	for _, epAdd := range ann.Add {
		addr := endpointFullAddress(epAdd)

		if err := r.balancer.AddEndpoint(
			[]string{epAdd.Location, addr},
			func() (*transport.Connection, error) {
				return transport.NewConnection(ctx, addr, r.transportCredentials, r.auth, r.db)
			}); err != nil {
			r.logger.Error("unable to add endpoint", "error", err)
		} else {
			r.logger.Debug("endpoint added", "address", addr)
		}
	}

	for _, epDel := range ann.Del {
		addr := endpointFullAddress(epDel)

		if err := r.balancer.DeleteEndpoint([]string{epDel.Location, addr}); err != nil {
			r.logger.Error("unable to delete endpoint", "error", err)
		} else {
			r.logger.Debug("endpoint deleted", "address", addr)
		}
	}
}

type addrPort interface {
	GetAddress() string
	GetPort() uint32
}

func endpointFullAddress(ep addrPort) string {
	return ep.GetAddress() + ":" + strconv.Itoa(int(ep.GetPort()))
}
