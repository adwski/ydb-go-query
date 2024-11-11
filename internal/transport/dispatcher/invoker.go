package dispatcher

import (
	"context"
	"errors"

	localErrs "github.com/adwski/ydb-go-query/internal/errors"
	"github.com/adwski/ydb-go-query/internal/transport"
	balancing "github.com/adwski/ydb-go-query/internal/transport/balancing/v3"
	"github.com/adwski/ydb-go-query/internal/xcontext"

	"google.golang.org/grpc"
)

type invoker struct {
	*balancing.Grid[*transport.Connection, transport.Connection]
}

func newInvoker(grid *balancing.Grid[*transport.Connection, transport.Connection]) *invoker {
	return &invoker{Grid: grid}
}

func (inv *invoker) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	trPtr := xcontext.GetTransportPtr(ctx)
	if conn := inv.GetConn(); conn != nil {
		if trPtr != nil {
			*trPtr = conn
		}

		return conn.Invoke(ctx, method, args, reply, opts...) //nolint:wrapcheck // unnecessary
	}

	return errors.Join(localErrs.LocalFailureError{}, ErrNoConnections)
}

func (inv *invoker) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if conn := inv.GetConn(); conn != nil {
		return conn.NewStream(ctx, desc, method, opts...) //nolint:wrapcheck // unnecessary
	}

	return nil, errors.Join(localErrs.LocalFailureError{}, ErrNoConnections)
}
