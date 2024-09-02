package transaction

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/query/session"
	"github.com/adwski/ydb-go-query/v1/query/result"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
)

var (
	ErrFinished = errors.New("transaction already finished")
	ErrPrepare  = errors.New("unable to prepare transaction")
	ErrResult   = errors.New("result fetch error")
	ErrExec     = errors.New("query execution failed")
	ErrCommited = errors.New("commited transaction")
)

type (
	Transaction struct {
		logger logger.Logger

		sess *session.Session

		cleanup func()

		settings *Ydb_Query.TransactionSettings

		id string

		finish atomic.Bool

		commit bool
	}
)

func (tx *Transaction) Rollback(ctx context.Context) error {
	if tx.commit {
		return ErrCommited
	}

	if err := tx.sess.RollbackTX(ctx, tx.id); err != nil {
		return err
	}

	return nil
}

func (tx *Transaction) Do(
	ctx context.Context,
	query string,
	params map[string]*Ydb.TypedValue,
	collectRowsFunc func([]*Ydb.Value),
	commit bool,
) (*result.Result, error) {
	if tx.finish.Load() {
		return nil, ErrFinished
	}

	if commit {
		defer func() {
			tx.finish.Store(true)
			tx.cleanup()
		}()
	}

	txControl := &Ydb_Query.TransactionControl{
		// send last exec with commit
		CommitTx: commit,
	}
	if tx.id == "" {
		// begin tx
		txControl.TxSelector = &Ydb_Query.TransactionControl_BeginTx{
			BeginTx: tx.settings,
		}
	} else {
		// continue tx
		txControl.TxSelector = &Ydb_Query.TransactionControl_TxId{
			TxId: tx.id,
		}
	}

	res, err := tx.sess.Exec(ctx, query, params, txControl, collectRowsFunc)

	if err != nil {
		return nil, errors.Join(ErrExec, err)
	}

	if err = res.Recv(); err != nil {
		return nil, errors.Join(ErrResult, err)
	}

	tx.id = res.TxID()
	tx.logger.Trace("received tx result", "txID", tx.id)

	return res, nil
}

func (tx *Transaction) exec(
	ctx context.Context,
	query string,
	params map[string]*Ydb.TypedValue,
	collectRowsFunc func([]*Ydb.Value),
) (*result.Result, error) {
	if tx.finish.Load() {
		return nil, ErrFinished
	}
	defer func() {
		tx.finish.Store(true)
		tx.cleanup()
	}()

	// begin tx
	txControl := &Ydb_Query.TransactionControl{
		TxSelector: &Ydb_Query.TransactionControl_BeginTx{
			BeginTx: tx.settings,
		},
		CommitTx: tx.commit,
	}
	res, err := tx.sess.Exec(ctx, query, params, txControl, collectRowsFunc)

	if err != nil {
		return nil, errors.Join(ErrExec, err)
	}

	if err = res.Recv(); err != nil {
		return nil, errors.Join(ErrResult, err)
	}

	tx.id = res.TxID()

	return res, nil
}
