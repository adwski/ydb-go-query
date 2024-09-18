package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/adwski/ydb-go-query/internal/endpoints"
	"github.com/adwski/ydb-go-query/internal/logger"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"google.golang.org/grpc"
)

var (
	ErrEndpointsList         = errors.New("unable to get endpoints")
	ErrEndpointsUnmarshal    = errors.New("unable to unmarshal endpoints")
	ErrOperationUnsuccessful = errors.New("operation unsuccessful")
)

const (
	discoveryTimeout  = 3 * time.Second
	discoveryInterval = 30 * time.Second
	discoveryErrRetry = 2 * time.Second
)

type (
	Service struct {
		logger logger.Logger
		dsc    Ydb_Discovery_V1.DiscoveryServiceClient
		ann    chan endpoints.Announce
		filter *endpoints.Filter
		epDB   endpoints.DB
		dbName string
	}

	Config struct {
		Logger     logger.Logger
		Transport  grpc.ClientConnInterface
		DB         string
		DoAnnounce bool
	}
)

func NewService(cfg Config) *Service {
	svc := &Service{
		dbName: cfg.DB,
		logger: cfg.Logger,
		filter: endpoints.NewFilter().WithQueryService(),
		dsc:    Ydb_Discovery_V1.NewDiscoveryServiceClient(cfg.Transport),
		epDB:   endpoints.NewDB(),
	}
	if cfg.DoAnnounce {
		svc.ann = make(chan endpoints.Announce)
	}

	return svc
}

func (svc *Service) EndpointsChan() <-chan endpoints.Announce {
	return svc.ann
}

func (svc *Service) GetAllEndpoints() endpoints.Map {
	return svc.epDB.GetAll()
}

func (svc *Service) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	waitTimer := svc.endpointsTick(ctx, nil)
	defer waitTimer.Stop()

runLoop:
	for {
		select {
		case <-ctx.Done():
			break runLoop
		case <-waitTimer.C:
			svc.endpointsTick(ctx, waitTimer)
		}
	}
}

func (svc *Service) endpointsTick(ctx context.Context, waitTimer *time.Timer) *time.Timer {
	ctxEp, cancelEp := context.WithDeadline(ctx, time.Now().Add(discoveryTimeout))
	defer cancelEp()

	timerInterval := discoveryInterval

	if eps, err := svc.getEndpoints(ctxEp); err != nil {
		svc.logger.Error("getEndpoints failed", "error", err, "db", svc.dbName)
		timerInterval = discoveryErrRetry
	} else {
		svc.logger.Debug("getEndpoints succeeded", "count", len(eps))
		svc.updateAndAnnounce(ctx, eps)
	}

	if waitTimer == nil {
		return time.NewTimer(timerInterval)
	}
	waitTimer.Reset(timerInterval)

	return nil
}

func (svc *Service) updateAndAnnounce(ctx context.Context, endpoints []*Ydb_Discovery.EndpointInfo) {
	if svc.epDB.Compare(endpoints) {
		// endpoints did not change
		return
	}

	announce, oldLen, newLen := svc.epDB.Update(endpoints)

	svc.logger.Info("endpoints changed",
		"was", oldLen,
		"now", newLen,
		"new", len(announce.Add),
		"old", len(announce.Del))

	if svc.ann == nil {
		return
	}

	select {
	case <-ctx.Done():
	case svc.ann <- announce:
	}
}

func (svc *Service) getEndpoints(ctx context.Context) ([]*Ydb_Discovery.EndpointInfo, error) {
	resp, err := svc.dsc.ListEndpoints(ctx, &Ydb_Discovery.ListEndpointsRequest{
		Database: svc.dbName,
	})
	if err != nil {
		return nil, errors.Join(ErrEndpointsList, err)
	}
	status := resp.GetOperation().GetStatus()
	if status != Ydb.StatusIds_SUCCESS {
		return nil, errors.Join(ErrOperationUnsuccessful,
			fmt.Errorf("%s", resp.GetOperation().String()))
	}
	var epRes Ydb_Discovery.ListEndpointsResult
	if err = resp.GetOperation().GetResult().UnmarshalTo(&epRes); err != nil {
		return nil, errors.Join(ErrEndpointsUnmarshal, err)
	}

	preferred, requiredButNotPreferred := svc.filter.Filter(epRes.Endpoints)
	if len(preferred) == 0 {
		return requiredButNotPreferred, nil
	}

	return preferred, nil
}
