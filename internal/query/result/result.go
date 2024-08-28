package result

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

var (
	ErrPartStatus = errors.New("result part status error")
	ErrStream     = errors.New("result stream error")
)

type Result struct {
	logger logger.Logger

	cancel context.CancelFunc

	stream Ydb_Query_V1.QueryService_ExecuteQueryClient

	stats *Ydb_TableStats.QueryStats

	err error

	issues []*Ydb_Issue.IssueMessage

	cols []*Ydb.Column
	rows []*Ydb.Value

	done atomic.Bool
}

func NewResult(
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	cancel context.CancelFunc,
	logger logger.Logger,
) *Result {
	return &Result{
		logger: logger,
		stream: stream,
		cancel: cancel,
	}
}

// Close closes result stream,
// but result data is still available to be read.
func (r *Result) Close() {
	r.cancel()
	r.done.Store(true)
}

func (r *Result) Err() error {
	return r.err
}

func (r *Result) Issues() []*Ydb_Issue.IssueMessage { return r.issues }

func (r *Result) Cols() []*Ydb.Column {
	return r.cols
}

func (r *Result) Rows() []*Ydb.Value {
	return r.rows
}

func (r *Result) Stats() *Ydb_TableStats.QueryStats {
	return r.stats
}

// ReceiveAll reads all parts from result stream.
// It assumes that parts are arriving sequentially,
// i.e. ConcurrentResultSets is false.
func (r *Result) ReceiveAll() error {
	if r.done.Load() {
		return nil
	}
	for {
		part, err := r.stream.Recv()
		r.logger.Trace("received result part", "part", part, "error", err)
		if err != nil {
			return errors.Join(ErrStream, err)
		}

		if part.Status != Ydb.StatusIds_SUCCESS {
			r.issues = append(r.issues, part.Issues...)
			r.err = errors.Join(ErrPartStatus, fmt.Errorf("status: %s", part.Status))

			break
		}

		if part.ResultSet != nil {
			if r.cols == nil && len(part.ResultSet.Columns) > 0 {
				r.cols = make([]*Ydb.Column, len(part.ResultSet.Columns))
				copy(r.cols, part.ResultSet.GetColumns())
			}

			if len(part.ResultSet.Rows) > 0 {
				r.rows = append(r.rows, part.ResultSet.Rows...)
			}
		}

		if part.ExecStats != nil {
			// stats on the last part
			// TODO: find better way to detect last part
			r.stats = part.ExecStats
			break
		}
	}

	r.Close()

	return nil
}
