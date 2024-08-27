package transport

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	headerAuth     = "x-ydb-auth-ticket"
	headerDatabase = "x-ydb-database"
)

var (
	ErrNoToken = errors.New("authenticator did not provide token")

	ErrDial  = errors.New("connection dial error")
	ErrClose = errors.New("connection close error")
)

type (
	Authenticator interface {
		GetToken() string
	}
	Connection struct {
		*grpc.ClientConn

		auth Authenticator
		db   string
	}
)

func NewConnection(
	ctx context.Context,
	endpoint string,
	creds credentials.TransportCredentials,
	auth Authenticator,
	db string,
) (*Connection, error) {
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithTransportCredentials(creds))

	grpcConn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		return nil, errors.Join(ErrDial, err)
	}

	return &Connection{
		ClientConn: grpcConn,
		auth:       auth,
		db:         db,
	}, nil
}

func (c *Connection) Close() error {
	if err := c.ClientConn.Close(); err != nil {
		return errors.Join(ErrClose, err)
	}
	return nil
}

func (c *Connection) Alive() bool {
	switch c.GetState() {
	case connectivity.Ready, connectivity.Idle:
		return true
	default:
		return false
	}
}

func setContextMD(ctx context.Context, auth Authenticator, db string) (context.Context, error) {
	md := metadata.Pairs(headerDatabase, db)
	if auth != nil {
		token := auth.GetToken()
		if token == "" {
			return nil, ErrNoToken
		}
		md.Append(headerAuth, token)
	}

	return metadata.NewOutgoingContext(ctx, md), nil
}

func (c *Connection) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	callCtx, err := setContextMD(ctx, c.auth, c.db)
	if err != nil {
		return err
	}
	return c.ClientConn.Invoke(callCtx, method, args, reply, opts...) //nolint:wrapcheck // unnecessary
}

func (c *Connection) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	callCtx, err := setContextMD(ctx, c.auth, c.db)
	if err != nil {
		return nil, err
	}
	return c.ClientConn.NewStream(callCtx, desc, method, opts...) //nolint:wrapcheck // unnecessary
}
