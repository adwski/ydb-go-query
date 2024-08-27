package query

import (
	"context"
	"errors"
	"time"

	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/query/pool"
	"github.com/adwski/ydb-go-query/v1/internal/query/result"
	"github.com/adwski/ydb-go-query/v1/internal/query/session"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
)

var (
	ErrExec      = errors.New("query execution failed")
	ErrResult    = errors.New("result fetch error")
	ErrNoSession = errors.New("no session")
)

type Service struct {
	logger logger.Logger
	qsc    Ydb_Query_V1.QueryServiceClient

	pool *pool.Pool[*session.Session, session.Session]
}

type Config struct {
	Logger        logger.Logger
	Transport     grpc.ClientConnInterface
	CreateTimeout time.Duration
	PoolSize      uint
}

func NewService(runCtx context.Context, cfg Config) *Service {
	qsc := Ydb_Query_V1.NewQueryServiceClient(cfg.Transport)

	sessionPool := pool.New[*session.Session, session.Session](
		runCtx,
		pool.Config[*session.Session, session.Session]{
			Logger:        cfg.Logger,
			CreateTimeout: cfg.CreateTimeout,
			PoolSize:      cfg.PoolSize,
			CreateFunc: func(sessCtx context.Context, timeout time.Duration) (*session.Session, error) {
				return session.CreateSession(sessCtx, qsc, cfg.Logger, timeout)
			},
		})

	return &Service{
		logger: cfg.Logger,
		qsc:    qsc,
		pool:   sessionPool,
	}
}

func (svc *Service) Close() error {
	return svc.pool.Close() //nolint:wrapcheck // unnecessary
}

func (svc *Service) Exec(ctx context.Context, query string, params map[string]*Ydb.TypedValue) (*result.Result, error) {
	sess := svc.pool.Get(ctx)
	defer func() {
		// get will return nil if canceled
		if sess != nil {
			svc.pool.Put(sess)
		}
	}()

	if sess == nil {
		return nil, ErrNoSession
	}

	res, err := sess.Exec(ctx, query, params, nil)
	if err != nil {
		return nil, errors.Join(ErrExec, err)
	}

	svc.logger.Trace("received result stream", "query", query)

	res.ReceiveAll()

	if res.Err() != nil {
		return nil, errors.Join(ErrResult, err)
	}

	return res, nil
}
