package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
)

type (
	execFunc func(
		context.Context,
		string,
		map[string]*Ydb.TypedValue,
		*Ydb_Query.TransactionSettings,
		func([]*Ydb.Value) error,
	) (*Result, error)

	Query struct {
		txSet           *Ydb_Query.TransactionSettings
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

func (q *Query) CollectRows(collectRowsFunc func([]*Ydb.Value) error) *Query {
	q.collectRowsFunc = collectRowsFunc

	return q
}

func (q *Query) Exec(ctx context.Context) (*Result, error) {
	return q.execFunc(ctx, q.content, q.params, q.txSet, q.collectRowsFunc)
}

func (q *Query) OnlineReadOnly() *Query {
	q.txSet = &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: &Ydb_Query.OnlineModeSettings{},
		},
	}

	return q
}

func (q *Query) OnlineReadOnlyInconsistent() *Query {
	q.txSet = &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: &Ydb_Query.OnlineModeSettings{
				AllowInconsistentReads: true,
			},
		},
	}

	return q
}

func (q *Query) SnapshotReadOnly() *Query {
	q.txSet = &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_SnapshotReadOnly{
			SnapshotReadOnly: &Ydb_Query.SnapshotModeSettings{},
		},
	}

	return q
}

func (q *Query) StaleReadOnly() *Query {
	q.txSet = &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_StaleReadOnly{
			StaleReadOnly: &Ydb_Query.StaleModeSettings{},
		},
	}

	return q
}

func (q *Query) SerializableReadWrite() *Query {
	q.txSet = &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
			SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
		},
	}

	return q
}
