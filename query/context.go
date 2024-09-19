package query

import (
	"context"
	"errors"
	"time"

	"github.com/adwski/ydb-go-query/internal/logger"
	"github.com/adwski/ydb-go-query/internal/query"
	"github.com/adwski/ydb-go-query/internal/query/txsettings"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
)

const (
	maxQueryLogLength = 1000
)

var (
	queryLogCut = []byte("...")
)

type Ctx struct {
	logger  logger.Logger
	qSvc    *query.Service
	txSet   *Ydb_Query.TransactionSettings
	timeout time.Duration
}

func NewCtx(
	logger logger.Logger,
	qSvc *query.Service,
	txSet *Ydb_Query.TransactionSettings,
	timeout time.Duration,
) *Ctx {
	return &Ctx{
		logger:  logger,
		qSvc:    qSvc,
		txSet:   txSet,
		timeout: timeout,
	}
}

func (qc *Ctx) OnlineReadOnly() *Ctx {
	newQCtx := *qc
	newQCtx.txSet = txsettings.OnlineReadOnly()

	return &newQCtx
}

func (qc *Ctx) OnlineReadOnlyInconsistent() *Ctx {
	newQCtx := *qc
	newQCtx.txSet = txsettings.OnlineReadOnlyInconsistent()

	return &newQCtx
}

func (qc *Ctx) SnapshotReadOnly() *Ctx {
	newQCtx := *qc
	newQCtx.txSet = txsettings.SnapshotReadOnly()

	return &newQCtx
}

func (qc *Ctx) StaleReadOnly() *Ctx {
	newQCtx := *qc
	newQCtx.txSet = txsettings.StaleReadOnly()

	return &newQCtx
}

func (qc *Ctx) SerializableReadWrite() *Ctx {
	newQCtx := *qc
	newQCtx.txSet = txsettings.SerializableReadWrite()
	return &newQCtx
}

func (qc *Ctx) Query(queryContent string) *Query {
	return newQuery(
		queryContent,
		func(
			ctx context.Context,
			queryContent string,
			params map[string]*Ydb.TypedValue,
			collectRows func([]*Ydb.Value) error,
			timeout time.Duration,
		) (*Result, error) {
			return qc.exec(ctx, queryContent, params, collectRows, qc.txSet, timeout)
		},
	)
}

func (qc *Ctx) Exec(ctx context.Context, queryContent string) (*Result, error) {
	return qc.exec(ctx, queryContent, nil, nil, nil, 0)
}

func (qc *Ctx) exec(
	ctx context.Context,
	queryContent string,
	params map[string]*Ydb.TypedValue,
	collectRows func([]*Ydb.Value) error,
	txSet *Ydb_Query.TransactionSettings,
	timeout time.Duration,
) (*Result, error) {
	var (
		qCancel context.CancelFunc
	)
	if timeout == 0 {
		timeout = qc.timeout
	}
	if timeout > 0 {
		ctx, qCancel = context.WithDeadline(ctx, time.Now().Add(timeout))
		defer qCancel()
	}
	stream, cancel, err := qc.qSvc.Exec(ctx, queryContent, params, txSet)
	if err != nil {
		return nil, err //nolint:wrapcheck //unnecessary
	}

	qc.logger.Trace("received result stream", "query", strip(queryContent))

	return qc.processResult(stream, cancel, collectRows)
}

func (qc *Ctx) Tx(ctx context.Context) (*Transaction, error) {
	sess, cleanup, err := qc.qSvc.AcquireSession(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck //unnecessary
	}

	tx := &Transaction{
		logger:   qc.logger,
		settings: qc.txSet,
		sess:     sess,
		cleanup:  cleanup,
	}

	return tx, nil
}

func (qc *Ctx) processResult(
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	cancel context.CancelFunc,
	collectRows func([]*Ydb.Value) error,
) (*Result, error) {
	res := newResult(stream, cancel, qc.logger, collectRows)

	if err := res.recv(); err != nil {
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
