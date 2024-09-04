package query

import (
	"context"
	"errors"
	"time"

	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/pool"
	"github.com/adwski/ydb-go-query/v1/internal/query/session"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
)

var (
	ErrNoSession = errors.New("no session")
	ErrExec      = errors.New("exec failed")
)

type (
	Service struct {
		logger logger.Logger
		qsc    Ydb_Query_V1.QueryServiceClient

		pool *pool.Pool[*session.Session, session.Session]
	}
)

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

	svc := &Service{
		logger: cfg.Logger,
		qsc:    qsc,
		pool:   sessionPool,
	}

	return svc
}

func (svc *Service) Close() error {
	return svc.pool.Close() //nolint:wrapcheck //unnecessary
}

func (svc *Service) AcquireSession(ctx context.Context) (*session.Session, func(), error) {
	sess := svc.pool.Get(ctx)
	if sess == nil {
		return nil, nil, ErrNoSession
	}

	return sess, func() { svc.pool.Put(sess) }, nil
}

// Exec provides low-level single query execution.
func (svc *Service) Exec(
	ctx context.Context,
	query string,
	params map[string]*Ydb.TypedValue,
	txSettings *Ydb_Query.TransactionSettings,
) (Ydb_Query_V1.QueryService_ExecuteQueryClient, context.CancelFunc, error) {
	sess, cleanup, err := svc.AcquireSession(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	var txControl *Ydb_Query.TransactionControl
	if txSettings != nil {
		txControl = &Ydb_Query.TransactionControl{
			TxSelector: &Ydb_Query.TransactionControl_BeginTx{
				BeginTx: txSettings,
			},
			CommitTx: true,
		}
	}

	stream, cancel, err := sess.Exec(ctx, query, params, txControl)
	if err != nil {
		return nil, nil, errors.Join(ErrExec, err)
	}

	return stream, cancel, nil
}
