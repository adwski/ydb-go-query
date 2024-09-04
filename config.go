package ydbgoquery

import (
	"context"
	"errors"
	"time"

	"github.com/adwski/ydb-go-query/v1/internal/logger"
	"github.com/adwski/ydb-go-query/v1/internal/logger/noop"
	zerologger "github.com/adwski/ydb-go-query/v1/internal/logger/zerolog"
	"github.com/adwski/ydb-go-query/v1/internal/query/txsettings"
	"github.com/adwski/ydb-go-query/v1/internal/transport"
	"github.com/adwski/ydb-go-query/v1/internal/transport/auth"
	"github.com/adwski/ydb-go-query/v1/internal/transport/auth/userpass"
	"github.com/adwski/ydb-go-query/v1/internal/transport/auth/yc"
	transportCreds "github.com/adwski/ydb-go-query/v1/internal/transport/credentials"

	"github.com/rs/zerolog"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
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
		auth                 authRunner

		txSettings *Ydb_Query.TransactionSettings

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
	cfg.txSettings = txsettings.SerializableReadWrite()
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
		ycAuth, err := yc.New(ctx, ycCfg)
		if err != nil {
			return err //nolint:wrapcheck // unnecessary
		}
		cfg.auth = auth.New(ctx, auth.Config{
			Logger:   cfg.logger,
			Provider: ycAuth,
		})

		return nil
	}
}

var ErrAuthTransport = errors.New("unable to create auth transport")

func WithUserPass(username, password string) Option {
	return func(ctx context.Context, cfg *Config) error {
		tr, err := transport.NewConnection(ctx, cfg.InitialNodes[0], cfg.transportCredentials, nil, cfg.DB)
		if err != nil {
			return errors.Join(ErrAuthTransport, err)
		}
		cfg.auth = auth.New(ctx, auth.Config{
			Logger: cfg.logger,
			Provider: userpass.New(userpass.Config{
				Transport: tr,
				Username:  username,
				Password:  password,
			}),
		})

		return nil
	}
}

func WithSerializableReadWrite() Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.txSettings = txsettings.SerializableReadWrite()
		return nil
	}
}

func WithOnlineReadOnly() Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.txSettings = txsettings.OnlineReadOnly()
		return nil
	}
}

func WithOnlineReadOnlyInconsistent() Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.txSettings = txsettings.OnlineReadOnlyInconsistent()
		return nil
	}
}

func WithStaleReadOnly() Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.txSettings = txsettings.StaleReadOnly()
		return nil
	}
}

func WithSnapshotReadOnly() Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.txSettings = txsettings.SnapshotReadOnly()
		return nil
	}
}
