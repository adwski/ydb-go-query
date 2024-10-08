package query

import (
	"context"
	"errors"
	"time"

	"github.com/adwski/ydb-go-query/internal/logger"
	"github.com/adwski/ydb-go-query/internal/query/session"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
)

var (
	ErrTxFinished = errors.New("transaction already finished")
)

type (
	Transaction struct {
		logger logger.Logger

		sess *session.Session

		cleanup func()

		settings *Ydb_Query.TransactionSettings

		id string

		finish bool // committed or rolled back
	}
)

func (tx *Transaction) Rollback(ctx context.Context) error {
	if tx.finish {
		return ErrTxFinished
	}

	if err := tx.sess.RollbackTX(ctx, tx.id); err != nil {
		return err //nolint:wrapcheck // unnecessary
	}

	tx.finish = true
	tx.cleanup()

	return nil
}

func (tx *Transaction) Commit(ctx context.Context) error {
	if tx.finish {
		return ErrTxFinished
	}

	if err := tx.sess.CommitTX(ctx, tx.id); err != nil {
		return err //nolint:wrapcheck // unnecessary
	}

	tx.finish = true
	tx.cleanup()

	return nil
}

func (tx *Transaction) Query(queryContent string) *TxQuery {
	return newTxQuery(
		queryContent,
		tx.exec,
	)
}

func (tx *Transaction) exec(
	ctx context.Context,
	query string,
	params map[string]*Ydb.TypedValue,
	collectRowsFunc func([]*Ydb.Value) error,
	timeout time.Duration,
	commit bool,
) (*Result, error) {
	if tx.finish {
		return nil, ErrTxFinished
	}

	if commit {
		defer func() {
			tx.finish = true
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

	var (
		qCancel context.CancelFunc
	)
	if timeout > 0 {
		ctx, qCancel = context.WithDeadline(ctx, time.Now().Add(timeout))
	}

	stream, cancel, err := tx.sess.Exec(ctx, query, params, txControl)
	if qCancel != nil {
		// if ctx was overwritten, then cancel() inherits from qCancel()
		cancel = qCancel
	}
	if err != nil {
		cancel()
		return nil, err //nolint:wrapcheck //unnecessary
	}

	res := newResult(stream, cancel, tx.logger, collectRowsFunc)

	if err = res.recv(); err != nil {
		return nil, errors.Join(ErrResult, err)
	}

	tx.id = res.TxID()
	tx.logger.Trace("received tx result", "txID", tx.id)

	return res, nil
}
