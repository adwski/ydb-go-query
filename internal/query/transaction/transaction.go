package transaction

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/result"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
)

var (
	ErrFinished = errors.New("transaction already finished")
	ErrPrepare  = errors.New("unable to prepare transaction")
	ErrResult   = errors.New("result fetch error")
	ErrExec     = errors.New("query execution failed")
)

type (
	ExecFunc func(
		context.Context,
		string,
		map[string]*Ydb.TypedValue,
		*Ydb_Query.TransactionControl,
	) (*result.Result, error)

	prepareFunc func(ctx context.Context) (ExecFunc, func(), error)

	Settings struct {
		logger logger.Logger
		txSet  *Ydb_Query.TransactionSettings
		// execFunc    ExecFunc
		prepareFunc prepareFunc
	}

	Transaction struct {
		logger logger.Logger

		execFunc ExecFunc
		cleanup  func()

		settings *Ydb_Query.TransactionSettings

		id string

		finish atomic.Bool

		commit bool
	}
)

func New(log logger.Logger, pF prepareFunc) *Settings {
	return &Settings{
		logger:      log,
		prepareFunc: pF,
	}
}

func (s *Settings) ExecOne(
	ctx context.Context,
	query string,
	params map[string]*Ydb.TypedValue,
) (*result.Result, error) {
	if s.txSet == nil {
		s.SerializableReadWrite()
	}

	execFunc, cleanup, err := s.prepareFunc(ctx)
	if err != nil {
		return nil, errors.Join(ErrPrepare, err)
	}

	tx := &Transaction{
		logger:   s.logger,
		execFunc: execFunc,
		cleanup:  cleanup,
		settings: s.txSet,
		commit:   true,
	}

	return tx.exec(ctx, query, params)
}

func (s *Settings) ExecMany(ctx context.Context) (*Transaction, error) {
	if s.txSet == nil {
		s.SerializableReadWrite()
	}

	execFunc, cleanup, err := s.prepareFunc(ctx)
	if err != nil {
		return nil, errors.Join(ErrPrepare, err)
	}

	tx := &Transaction{
		logger:   s.logger,
		execFunc: execFunc,
		cleanup:  cleanup,
		settings: s.txSet,
	}

	return tx, nil
}

func (s *Settings) OnlineReadOnly() *Settings {
	s.txSet = &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: &Ydb_Query.OnlineModeSettings{},
		},
	}

	return s
}

func (s *Settings) OnlineReadOnlyInconsistent() *Settings {
	s.txSet = &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: &Ydb_Query.OnlineModeSettings{
				AllowInconsistentReads: true,
			},
		},
	}

	return s
}

func (s *Settings) SnapshotReadOnly() *Settings {
	s.txSet = &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_SnapshotReadOnly{
			SnapshotReadOnly: &Ydb_Query.SnapshotModeSettings{},
		},
	}

	return s
}

func (s *Settings) StaleReadOnly() *Settings {
	s.txSet = &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_StaleReadOnly{
			StaleReadOnly: &Ydb_Query.StaleModeSettings{},
		},
	}

	return s
}

func (s *Settings) SerializableReadWrite() *Settings {
	s.txSet = &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
			SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
		},
	}

	return s
}

func (tx *Transaction) Do(
	ctx context.Context,
	query string,
	params map[string]*Ydb.TypedValue,
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

	res, err := tx.execFunc(ctx, query, params, txControl)

	if err != nil {
		return nil, errors.Join(ErrExec, err)
	}

	if err = res.ReceiveAll(); err != nil {
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
	res, err := tx.execFunc(ctx, query, params, txControl)

	if err != nil {
		return nil, errors.Join(ErrExec, err)
	}

	if err = res.ReceiveAll(); err != nil {
		return nil, errors.Join(ErrResult, err)
	}

	tx.id = res.TxID()

	return res, nil
}
