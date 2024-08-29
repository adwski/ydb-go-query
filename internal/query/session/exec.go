package session

import (
	"context"
	"errors"

	"github.com/adwski/ydb-go-query/v1/internal/query/result"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
)

const (
	defaultStatsMode   = Ydb_Query.StatsMode_STATS_MODE_BASIC
	defaultQuerySyntax = Ydb_Query.Syntax_SYNTAX_YQL_V1
	defaultExecMode    = Ydb_Query.ExecMode_EXEC_MODE_EXECUTE
)

var (
	ErrExec     = errors.New("exec error")
	ErrShutdown = errors.New("session is shut down")
)

func (s *Session) Exec(
	ctx context.Context,
	query string,
	params map[string]*Ydb.TypedValue,
	txControl *Ydb_Query.TransactionControl,
) (*result.Result, error) {
	if s.shutdown.Load() {
		return nil, ErrShutdown
	}

	streamCtx, cancelStream := context.WithCancel(ctx)

	respExec, err := s.qsc.ExecuteQuery(streamCtx, &Ydb_Query.ExecuteQueryRequest{
		SessionId: s.id,
		ExecMode:  defaultExecMode,
		TxControl: txControl,
		Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
			QueryContent: &Ydb_Query.QueryContent{
				Syntax: defaultQuerySyntax,
				Text:   query,
			},
		},
		Parameters:           params,
		StatsMode:            defaultStatsMode,
		ConcurrentResultSets: false,
	})

	if err != nil {
		cancelStream()
		return nil, errors.Join(ErrExec, err)
	}

	return result.NewResult(respExec, cancelStream, s.logger), nil
}
