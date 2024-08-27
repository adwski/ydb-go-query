package dispatcher

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/adwski/ydb-go-query/v1/internal/discovery"
	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/transport"
	"github.com/adwski/ydb-go-query/v1/internal/transport/balancing/grid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	ErrDiscoveryTransportCreate = errors.New("discovery transport create error")

	ErrSecurity = errors.New("transport security error")
)

type (
	grpcBalancer interface {
		grpc.ClientConnInterface

		AddEndpoint([]string, func() (*transport.Connection, error)) error
		DeleteEndpoint([]string) error
	}

	// Dispatcher provides dynamic transport layer by
	// gluing together discovery service and balancer.
	//
	// It spawns discovery service and uses its announces
	// to populate balancing grid.
	//
	// Balancing grid in turn is used by YDB services
	// as abstract transport.
	Dispatcher struct {
		logger   logger.Logger
		balancer grpcBalancer

		discoverySvc *discovery.Service

		transportCredentials credentials.TransportCredentials
		auth                 transport.Authenticator

		db        string
		initNodes []string

		initialized bool
	}

	Config struct {
		Logger               logger.Logger
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
		initNodes:            cfg.InitNodes,
		db:                   cfg.DB,
		auth:                 cfg.Auth,
		transportCredentials: cfg.TransportCredentials,

		balancer: grid.NewWithThreeLevels(cfg.GridConfig),
	}
}

func (r *Dispatcher) Transport() grpc.ClientConnInterface {
	return r.balancer
}

func (r *Dispatcher) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	wg.Add(1)
	go r.discoverySvc.Run(ctx, wg)

	select {
	case <-ctx.Done():
	case ann := <-r.discoverySvc.EndpointsAnn():
		r.processAnnounce(ctx, ann)
	}
}

func (r *Dispatcher) Init(ctx context.Context) error {
	discoveryTr, err := transport.NewConnection(ctx, r.initNodes[0], r.transportCredentials, r.auth, r.db)
	if err != nil {
		return errors.Join(ErrDiscoveryTransportCreate, err)
	}

	r.discoverySvc = discovery.NewService(discovery.Config{
		Logger:     r.logger,
		DB:         r.db,
		Transport:  discoveryTr,
		DoAnnounce: true,
	})

	r.initialized = true

	return nil
}

func (r *Dispatcher) processAnnounce(ctx context.Context, ann discovery.Announce) {
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
