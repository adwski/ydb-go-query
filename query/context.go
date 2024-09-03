package query

import (
	"context"
	"errors"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"

	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/query"
	"github.com/adwski/ydb-go-query/v1/internal/query/txsettings"

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
	logger logger.Logger
	qSvc   *query.Service
	txSet  *Ydb_Query.TransactionSettings
}

func NewCtx(logger logger.Logger, qSvc *query.Service, txSet *Ydb_Query.TransactionSettings) *Ctx {
	return &Ctx{
		logger: logger,
		qSvc:   qSvc,
		txSet:  txSet,
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
		qc.exec,
	)
}

func (qc *Ctx) Exec(ctx context.Context, queryContent string) (*Result, error) {
	stream, cancel, err := qc.qSvc.Exec(ctx, queryContent, nil, nil)
	if err != nil {
		return nil, err //nolint:wrapcheck //unnecessary
	}

	return qc.processResult(stream, cancel, nil)
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

func (qc *Ctx) exec(
	ctx context.Context,
	queryContent string,
	params map[string]*Ydb.TypedValue,
	collectRows func([]*Ydb.Value) error,
) (*Result, error) {
	stream, cancel, err := qc.qSvc.Exec(ctx, queryContent, params, qc.txSet)
	if err != nil {
		return nil, err //nolint:wrapcheck //unnecessary
	}

	qc.logger.Trace("received result stream", "query", strip(queryContent))

	return qc.processResult(stream, cancel, collectRows)
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
