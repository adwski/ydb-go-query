package ydbgoquery

import (
	"context"
	"time"

	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/logger/noop"
	zerologger "github.com/adwski/ydb-go-query/v1/internal/logger/zerolog"
	"github.com/adwski/ydb-go-query/v1/internal/transport"
	"github.com/adwski/ydb-go-query/v1/internal/transport/auth/yc"
	transportCreds "github.com/adwski/ydb-go-query/v1/internal/transport/credentials"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/credentials"
)

const (
	defaultSessionCreateTimeout = 3 * time.Second
	defaultSessionPoolSize      = 10
)

type (
	Config struct {
		logger               logger.Logger
		transportCredentials credentials.TransportCredentials
		auth                 transport.Authenticator

		DB           string
		InitialNodes []string

		poolSize             uint
		sessionCreateTimeout time.Duration
	}
	Option func(context.Context, *Config) error
)

func (cfg *Config) setDefaults() {
	cfg.logger = noop.NewLogger()
	cfg.sessionCreateTimeout = defaultSessionCreateTimeout
	cfg.poolSize = defaultSessionPoolSize
	cfg.transportCredentials = transportCreds.Insecure()
}

func WithLogger(log logger.Logger) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.logger = log
		return nil
	}
}

func WithZeroLogger(log zerolog.Logger) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.logger = zerologger.NewLogger(log)
		return nil
	}
}

func WithSessionCreateTimeout(timeout time.Duration) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.sessionCreateTimeout = timeout
		return nil
	}
}

func WithSessionPoolSize(size uint) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.poolSize = size
		return nil
	}
}

func WithTransportSecurity(credentials credentials.TransportCredentials) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.transportCredentials = credentials
		return nil
	}
}

func WithTransportTLS() Option {
	return WithTransportSecurity(transportCreds.TLS())
}

func WithAuth(auth transport.Authenticator) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.auth = auth
		return nil
	}
}

func WithYCAuthFile(filename string) Option {
	return withYC(yc.Config{
		IamKeyFile: filename,
	})
}

func WithYCAuthBytes(iamKeyBytes []byte) Option {
	return withYC(yc.Config{
		IamKey: iamKeyBytes,
	})
}

func withYC(ycCfg yc.Config) Option {
	return func(ctx context.Context, cfg *Config) error {
		auth, err := yc.New(ctx, cfg.logger, ycCfg)
		if err != nil {
			return err //nolint:wrapcheck // unnecessary
		}
		cfg.auth = auth
		return nil
	}
}
