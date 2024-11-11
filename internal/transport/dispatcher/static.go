package dispatcher

import (
	"context"
	"errors"

	"github.com/adwski/ydb-go-query/internal/transport"
	balancing "github.com/adwski/ydb-go-query/internal/transport/balancing/v3"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Static struct {
	invoker *invoker
}

// NewStatic provides transport layer with fixed endpoints.
func NewStatic(
	ctx context.Context,
	endpoints []string,
	creds credentials.TransportCredentials,
	auth transport.Authenticator,
	db string,
) (*Static, error) {
	grid := balancing.NewGrid[*transport.Connection, transport.Connection](balancing.Config{
		ConnsPerEndpoint: 1,
		IgnoreLocations:  true,
	})

	for _, addr := range endpoints {
		err := grid.Add("", func() (*transport.Connection, error) {
			return transport.NewConnection(ctx, addr, creds, auth, db, 0)
		})

		if err != nil {
			return nil, errors.Join(ErrCreateEndpoint, err)
		}
	}

	return &Static{invoker: newInvoker(grid)}, nil
}

func (s *Static) Transport() grpc.ClientConnInterface {
	return s.invoker
}
