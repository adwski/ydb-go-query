package query

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/query/pool"
	"github.com/adwski/ydb-go-query/v1/internal/query/session"
	"github.com/adwski/ydb-go-query/v1/query/result"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
)

const (
	maxQueryLogLength = 1000
)

var (
	ErrNoSession = errors.New("no session")

	queryLogCut = []byte("...")
)

type (
	Service struct {
		logger logger.Logger
		qsc    Ydb_Query_V1.QueryServiceClient

		wg *sync.WaitGroup

		pool *pool.Pool[*session.Session, session.Session]
	}
)

type Config struct {
	Logger        logger.Logger
	Transport     grpc.ClientConnInterface
	CreateTimeout time.Duration
	PoolSize      uint
}

func NewService(runCtx context.Context, wg *sync.WaitGroup, cfg Config) *Service {
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
		wg:     wg,
	}

	// wait for context cancellation
	go svc.waitClose(runCtx)

	return svc
}

// waitClose ensures pool is closed before signaling wg.Done() to the owner or query service.
func (svc *Service) waitClose(ctx context.Context) {
	<-ctx.Done()
	_ = svc.pool.Close()
	svc.wg.Done()
}

func (svc *Service) Exec(
	ctx context.Context,
	query string,
) (*result.Result, error) {
	return svc.exec(ctx, query, nil, nil, nil)
}

func (svc *Service) New(queryContent string) *Query {
	return newQuery(
		queryContent,
		svc.exec,
	)
}

func (svc *Service) Tx(ctx context.Context) (*Transaction, error) {
	sess := svc.pool.Get(ctx)
	if sess == nil {
		return nil, ErrNoSession
	}

	tx := &Transaction{
		logger: svc.logger,
		sess:   sess,
		cleanup: func() {
			svc.pool.Put(sess)
		},
		settings: &Ydb_Query.TransactionSettings{
			TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
				SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
			},
		},
	}

	return tx, nil
}

// exec provides low-level single query execution.
func (svc *Service) exec(
	ctx context.Context,
	query string,
	params map[string]*Ydb.TypedValue,
	txSettings *Ydb_Query.TransactionSettings,
	collectRows func([]*Ydb.Value) error,
) (*result.Result, error) {
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

	var txControl *Ydb_Query.TransactionControl
	if txSettings != nil {
		txControl = &Ydb_Query.TransactionControl{
			TxSelector: &Ydb_Query.TransactionControl_BeginTx{
				BeginTx: txSettings,
			},
			CommitTx: true,
		}
	}

	res, err := sess.Exec(ctx, query, params, txControl, collectRows)
	if err != nil {
		return nil, errors.Join(ErrExec, err)
	}

	svc.logger.Trace("received result stream", "query", strip(query))

	if err = res.Recv(); err != nil {
		return nil, errors.Join(ErrResult, err)
	}

	return res, nil
}

func strip(s string) string {
	if len(s) > maxQueryLogLength {
		b := []byte(s[:maxQueryLogLength-2])
		return string(append(b, queryLogCut...))
	}

	return s
}
