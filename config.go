package ydbgoquery

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/adwski/ydb-go-query/internal/logger"
	"github.com/adwski/ydb-go-query/internal/logger/noop"
	zaplogger "github.com/adwski/ydb-go-query/internal/logger/zap"
	zerologger "github.com/adwski/ydb-go-query/internal/logger/zerolog"
	"github.com/adwski/ydb-go-query/internal/query/txsettings"
	"github.com/adwski/ydb-go-query/internal/transport/auth"
	"github.com/adwski/ydb-go-query/internal/transport/auth/userpass"
	"github.com/adwski/ydb-go-query/internal/transport/auth/yc"
	transportCreds "github.com/adwski/ydb-go-query/internal/transport/credentials"
	"github.com/adwski/ydb-go-query/internal/transport/dispatcher"

	"github.com/rs/zerolog"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
)

const (
	defaultSessionCreateTimeout   = 3 * time.Second
	defaultQueryTimeout           = 5 * time.Minute
	defaultSessionPoolSize        = 10
	defaultConnectionsPerEndpoint = 2
)

var (
	ErrAuthentication = errors.New("authentication failed")
)

type (
	Config struct {
		logger               logger.Logger
		transportCredentials credentials.TransportCredentials
		auth                 authRunner

		txSettings *Ydb_Query.TransactionSettings

		DB           string
		InitialNodes []string

		locationPreference []string

		poolSize    uint
		poolReadyHi uint
		poolReadyLo uint

		connectionsPerEndpoint int

		sessionCreateTimeout time.Duration
		queryTimeout         time.Duration
	}
	Option func(context.Context, *Config) error
)

func (cfg *Config) setDefaults() {
	cfg.logger = logger.New(noop.NewLogger())
	cfg.sessionCreateTimeout = defaultSessionCreateTimeout
	cfg.queryTimeout = defaultQueryTimeout
	cfg.poolSize = defaultSessionPoolSize
	cfg.connectionsPerEndpoint = defaultConnectionsPerEndpoint
	cfg.transportCredentials = transportCreds.Insecure()
	cfg.txSettings = txsettings.SerializableReadWrite()
}

func WithLogger(log logger.Logger) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.logger = log
		return nil
	}
}

func WithZeroLogger(log zerolog.Logger, level string) Option {
	return func(ctx context.Context, cfg *Config) error {
		lg, err := logger.NewWithLevel(zerologger.NewLogger(log), level)
		if err != nil {
			return err //nolint:wrapcheck // unnecessary
		}
		cfg.logger = lg

		return nil
	}
}

func WithZapLogger(log *zap.Logger, level string) Option {
	return func(ctx context.Context, cfg *Config) error {
		lg, err := logger.NewWithLevel(zaplogger.NewLogger(log), level)
		if err != nil {
			return err //nolint:wrapcheck // unnecessary
		}
		cfg.logger = lg

		return nil
	}
}

func WithSessionCreateTimeout(timeout time.Duration) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.sessionCreateTimeout = timeout
		return nil
	}
}

func WithQueryTimeout(timeout time.Duration) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.queryTimeout = timeout
		return nil
	}
}

func WithSessionPoolSize(size uint) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.poolSize = size
		return nil
	}
}

func WithSessionPoolReadyThresholds(high, low uint) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.poolReadyHi = high
		cfg.poolReadyLo = low

		return nil
	}
}

func WithLocationPreference(pref string) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.locationPreference = strings.Split(pref, ",")
		for idx := range cfg.locationPreference {
			cfg.locationPreference[idx] = strings.TrimSpace(cfg.locationPreference[idx])
		}

		return nil
	}
}

func WithConnectionsPerEndpoint(connections int) Option {
	return func(ctx context.Context, cfg *Config) error {
		if connections > 0 {
			cfg.connectionsPerEndpoint = connections
		}

		return nil
	}
}

func withTransportSecurity(credentials credentials.TransportCredentials) Option {
	return func(ctx context.Context, cfg *Config) error {
		cfg.transportCredentials = credentials
		return nil
	}
}

func WithTransportTLS() Option {
	return withTransportSecurity(transportCreds.TLS())
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
		cfg.auth, err = auth.New(ctx, auth.Config{
			Logger:   cfg.logger,
			Provider: ycAuth,
		})

		if err != nil {
			return errors.Join(ErrAuthentication, err)
		}

		return nil
	}
}

var ErrAuthTransport = errors.New("unable to create auth transport")

func WithUserPass(username, password string) Option {
	return func(ctx context.Context, cfg *Config) error {
		tr, err := dispatcher.NewStatic(ctx, cfg.InitialNodes, cfg.transportCredentials, nil, cfg.DB)
		if err != nil {
			return errors.Join(ErrAuthTransport, err)
		}
		cfg.auth, err = auth.New(ctx, auth.Config{
			Logger: cfg.logger,
			Provider: userpass.New(userpass.Config{
				Transport: tr.Transport(),
				Username:  username,
				Password:  password,
			}),
		})

		if err != nil {
			return errors.Join(ErrAuthentication, err)
		}

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
