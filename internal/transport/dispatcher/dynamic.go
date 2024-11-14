package dispatcher

import (
	"context"
	"strconv"
	"sync"

	"github.com/adwski/ydb-go-query/internal/endpoints"
	"github.com/adwski/ydb-go-query/internal/logger"
	"github.com/adwski/ydb-go-query/internal/transport"
	balancing "github.com/adwski/ydb-go-query/internal/transport/balancing/v4"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type (
	EndpointsProvider interface {
		EndpointsChan() <-chan endpoints.Announce
	}

	// Dynamic is a dispatcher that provides dynamic transport layer by
	// gluing together endpoints provider and balancer.
	//
	// It uses provider's announces to populate balancing grid,
	// and acts as grpc transport.
	Dynamic struct {
		logger   logger.Logger
		balancer *balancing.Grid[*transport.Connection, transport.Connection]
		invoker  *invoker

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
		Balancing            balancing.Config
	}
)

func NewDynamic(cfg Config) *Dynamic {
	grid := balancing.NewGrid[*transport.Connection, transport.Connection](cfg.Balancing)
	return &Dynamic{
		logger:               cfg.Logger,
		discovery:            cfg.EndpointsProvider,
		transportCredentials: cfg.TransportCredentials,
		auth:                 cfg.Auth,
		db:                   cfg.DB,
		invoker:              newInvoker(grid),
		balancer:             grid,
	}
}

func (d *Dynamic) Run(ctx context.Context, wg *sync.WaitGroup) {
	d.logger.Debug("dispatcher started")
	defer func() {
		d.logger.Debug("dispatcher stopped")
		wg.Done()
	}()

runLoop:
	for {
		select {
		case <-ctx.Done():
			break runLoop
		case ann := <-d.discovery.EndpointsChan():
			d.processAnnounce(ctx, ann)
		}
	}
}

func (d *Dynamic) Transport() grpc.ClientConnInterface {
	return d.invoker
}

func (d *Dynamic) processAnnounce(ctx context.Context, ann endpoints.Announce) {
	for epAdd := range ann.Add {
		addr := endpointFullAddress(&epAdd)

		if err := d.balancer.Add(epAdd.Location, func() (*transport.Connection, error) {
			return transport.NewConnection(ctx, addr, d.transportCredentials, d.auth, d.db, epAdd.AddressHash)
		}); err != nil {
			d.logger.Error("unable to add endpoint", "error", err)
		} else {
			d.logger.Debug("endpoint added", "address", addr)
		}
	}

	for _, epDel := range ann.Del {
		addr := endpointFullAddress(&epDel)

		if err := d.balancer.Delete(epDel.Location, epDel.AddressHash); err != nil {
			d.logger.Error("unable to delete endpoint", "error", err)
		} else {
			d.logger.Debug("endpoint deleted", "address", addr)
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
