package query

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type (
	txExecFunc func(
		context.Context,
		string,
		map[string]*Ydb.TypedValue,
		func([]*Ydb.Value) error,
		time.Duration,
		bool,
	) (*Result, error)

	TxQuery struct {
		collectRowsFunc func([]*Ydb.Value) error
		txExecFunc      txExecFunc
		params          map[string]*Ydb.TypedValue
		content         string
		timeout         time.Duration
		commit          bool
	}
)

func newTxQuery(content string, eF txExecFunc) *TxQuery {
	return &TxQuery{
		content: content,

		txExecFunc: eF,
	}
}

func (q *TxQuery) Commit() *TxQuery {
	q.commit = true

	return q
}

func (q *TxQuery) Collect(collectRowsFunc func([]*Ydb.Value) error) *TxQuery {
	q.collectRowsFunc = collectRowsFunc

	return q
}

func (q *TxQuery) Param(name string, val *Ydb.TypedValue) *TxQuery {
	if q.params == nil {
		q.params = make(map[string]*Ydb.TypedValue)
	}
	q.params[name] = val

	return q
}

func (q *TxQuery) Params(params map[string]*Ydb.TypedValue) *TxQuery {
	q.params = params

	return q
}

func (q *TxQuery) Timeout(timeout time.Duration) *TxQuery {
	q.timeout = timeout

	return q
}

func (q *TxQuery) Exec(ctx context.Context) (*Result, error) {
	return q.txExecFunc(ctx, q.content, q.params, q.collectRowsFunc, q.timeout, q.commit)
}
