package query

import (
	"context"

	"github.com/adwski/ydb-go-query/v1/query/result"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type (
	txExecFunc func(
		context.Context,
		string,
		map[string]*Ydb.TypedValue,
		func([]*Ydb.Value) error,
		bool,
	) (*result.Result, error)

	TxQuery struct {
		*Query

		txExecFunc txExecFunc

		commit bool
	}
)

func newTxQuery(content string, eF txExecFunc) *TxQuery {
	return &TxQuery{
		Query: newQuery(content, nil),

		txExecFunc: eF,
	}
}

func (q *TxQuery) Commit() *TxQuery {
	q.commit = true

	return q
}

func (q *TxQuery) Exec(ctx context.Context) (*result.Result, error) {
	return q.txExecFunc(ctx, q.content, q.params, q.collectRowsFunc, q.commit)
}
