package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type (
	execFunc func(
		context.Context,
		string,
		map[string]*Ydb.TypedValue,
		func([]*Ydb.Value) error,
	) (*Result, error)

	Query struct {
		collectRowsFunc func([]*Ydb.Value) error
		execFunc        execFunc
		params          map[string]*Ydb.TypedValue
		content         string
	}
)

func newQuery(content string, eF execFunc) *Query {
	return &Query{
		content:  content,
		execFunc: eF,
	}
}

func (q *Query) Params(params map[string]*Ydb.TypedValue) *Query {
	q.params = params

	return q
}

func (q *Query) Param(name string, val *Ydb.TypedValue) *Query {
	if q.params == nil {
		q.params = make(map[string]*Ydb.TypedValue)
	}
	q.params[name] = val

	return q
}

func (q *Query) Collect(collectRowsFunc func([]*Ydb.Value) error) *Query {
	q.collectRowsFunc = collectRowsFunc

	return q
}

func (q *Query) Exec(ctx context.Context) (*Result, error) {
	return q.execFunc(ctx, q.content, q.params, q.collectRowsFunc)
}
