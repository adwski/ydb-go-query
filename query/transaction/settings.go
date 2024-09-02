package transaction

import (
	"context"
	"errors"
	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/query/session"
	"github.com/adwski/ydb-go-query/v1/query/result"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
)

type (
	/*
		SessFunc func(
			context.Context,
			string,
			map[string]*Ydb.TypedValue,
			*Ydb_Query.TransactionControl,
			func([]*Ydb.Value),
		) (*result.Result, error)
	*/

	prepareFunc func(ctx context.Context) (*session.Session, func(), error)

	Settings struct {
		logger      logger.Logger
		txSet       *Ydb_Query.TransactionSettings
		prepareFunc prepareFunc
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
	collectRowsFunc func([]*Ydb.Value),
) (*result.Result, error) {
	if s.txSet == nil {
		s.SerializableReadWrite()
	}

	sess, cleanup, err := s.prepareFunc(ctx)
	if err != nil {
		return nil, errors.Join(ErrPrepare, err)
	}

	tx := &Transaction{
		logger:   s.logger,
		sess:     sess,
		cleanup:  cleanup,
		settings: s.txSet,
		commit:   true,
	}

	return tx.exec(ctx, query, params, collectRowsFunc)
}

func (s *Settings) ExecMany(ctx context.Context) (*Transaction, error) {
	if s.txSet == nil {
		s.SerializableReadWrite()
	}

	sess, cleanup, err := s.prepareFunc(ctx)
	if err != nil {
		return nil, errors.Join(ErrPrepare, err)
	}

	tx := &Transaction{
		logger:   s.logger,
		sess:     sess,
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
