//go:build integration

package ydbgoquery

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/adwski/ydb-go-query/query"
	"github.com/adwski/ydb-go-query/types"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	ydbPath = "/local"

	logLevel = "debug"

	ydbDefaultUser     = "root"
	ydbDefaultPassword = "1234"

	usersTable = `CREATE TABLE users (                  
    	user_id Uint64,
    	first_name Utf8,
    	last_name Utf8,
    	email Utf8,
    	registered_ts Uint64,
    	PRIMARY KEY (user_id))`

	userUpsert = `DECLARE $user_id AS Uint64;
			DECLARE $first_name AS Utf8;
			DECLARE $last_name AS Utf8;
			DECLARE $email AS Utf8;
			DECLARE $registered_ts AS Uint64;
			UPSERT INTO users (
				user_id, first_name, last_name, email, registered_ts
			) VALUES (
				$user_id, $first_name, $last_name, $email, $registered_ts)`
)

var (
	ydbEndpoint = "127.0.0.1:2136"

	ydbLocalConfig      Config
	ydbServerlessConfig Config

	ydbServerlessIAMKey string

	zeroLogger = zerolog.New(zerolog.NewConsoleWriter()).
			Level(zerolog.DebugLevel).
			With().Timestamp().Logger()

	zapLogger = zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		TimeKey:        "time",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}), os.Stdout, zapcore.DebugLevel))
)

func init() {
	if val, ok := os.LookupEnv("YDB_TEST_ENDPOINT"); ok {
		ydbEndpoint = val
	}

	ydbLocalConfig = Config{
		InitialNodes: []string{ydbEndpoint},
		DB:           ydbPath,
	}

	ydbServerlessConfig = Config{
		InitialNodes: []string{"ydb.serverless.yandexcloud.net:2135"},
		DB:           os.Getenv("YDB_SERVERLESS_DB"),
	}

	ydbServerlessIAMKey = os.Getenv("YDB_SERVERLESS_IAM_KEY")
}

func TestClient_Open(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := Open(ctx, Config{
		InitialNodes: []string{ydbEndpoint},
		DB:           ydbPath,
	}, WithZeroLogger(zeroLogger, logLevel))
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		client.Close()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "close timeout")
	}
}

func TestClient_UserPass(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
		wantErr  error
	}{
		{
			name:     "success",
			username: ydbDefaultUser,
			password: ydbDefaultPassword,
		},
		{
			name:     "unauthorized",
			username: "wronguser",
			password: "wrongpassword",
			wantErr:  ErrAuthentication,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			client, err := Open(ctx,
				Config{
					InitialNodes: []string{ydbEndpoint},
					DB:           ydbPath,
				},
				WithZeroLogger(zeroLogger, logLevel),
				WithUserPass(tt.username, tt.password),
			)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, client)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, client)

			done := make(chan struct{})
			go func() {
				client.Close()
				done <- struct{}{}
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				require.Fail(t, "close timeout")
			}
		})
	}
}

func TestClient(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		options []Option
	}{
		{
			name:   "Local",
			config: ydbLocalConfig,
		},
		{
			name:   "Serverless",
			config: ydbServerlessConfig,
			options: []Option{
				WithTransportTLS(),
				WithYCAuthBytes([]byte(ydbServerlessIAMKey)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("Queries", func(t *testing.T) {
				testQueries(t, tt.config, tt.options)
			})
			t.Run("Transactions", func(t *testing.T) {
				t.Run("Successful", func(t *testing.T) {
					testTransactions(t, tt.config, tt.options)
				})
				t.Run("Finished", func(t *testing.T) {
					testTransactionFinished(t, tt.config, tt.options)
				})
			})
		})
	}
}

func testQueries(t *testing.T, cfg Config, opts []Option) {
	t.Helper()

	const (
		usersCount = 100
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts = append(opts,
		WithZeroLogger(zeroLogger, logLevel),
		WithQueryTimeout(5*time.Second),
	)

	client, err := Open(ctx, cfg, opts...)
	require.NoError(t, err)
	defer client.Close()

	qCtx := client.QueryCtx()

	prepareUsersTable(ctx, t, qCtx)

	var res *query.Result
	for i := 0; i < usersCount; i++ {
		res, err = qCtx.Query(userUpsert).
			Param("$user_id", types.Uint64(gofakeit.Uint64())).
			Param("$first_name", types.UTF8(gofakeit.FirstName())).
			Param("$last_name", types.UTF8(gofakeit.LastName())).
			Param("$email", types.UTF8(gofakeit.Email())).
			Param("$registered_ts", types.Uint64(gofakeit.Uint64())).
			Exec(ctx)
		verifyResult(t, res, err)
	}

	// try different tx modes
	assertUsers(ctx, t, qCtx.SerializableReadWrite(), usersCount)
	assertUsers(ctx, t, qCtx.SnapshotReadOnly(), usersCount)
	assertUsers(ctx, t, qCtx.OnlineReadOnlyInconsistent(), usersCount)
	assertUsers(ctx, t, qCtx.OnlineReadOnly(), usersCount)
	assertUsers(ctx, t, qCtx.StaleReadOnly(), usersCount)
	assertUsers(ctx, t, qCtx, usersCount)

	dropUsersTable(ctx, t, qCtx)
}

func testTransactionFinished(t *testing.T, cfg Config, opts []Option) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts = append(opts,
		WithZeroLogger(zeroLogger, logLevel),
		WithQueryTimeout(5*time.Second))

	client, err := Open(ctx, cfg, opts...)
	require.NoError(t, err)
	defer client.Close()

	qCtx := client.QueryCtx()

	prepareUsersTable(ctx, t, qCtx)

	tx, err := qCtx.Tx(ctx)
	require.NoError(t, err)
	require.NotNil(t, tx)

	res, err := txQuery(tx).Commit().Exec(ctx)
	verifyResult(t, res, err)

	res, err = txQuery(tx).Commit().Exec(ctx)
	require.ErrorIs(t, err, query.ErrTxFinished)
	require.Nil(t, res)

	require.ErrorIs(t, tx.Commit(ctx), query.ErrTxFinished)
	require.ErrorIs(t, tx.Rollback(ctx), query.ErrTxFinished)

	dropUsersTable(ctx, t, qCtx)
}

func testTransactions(t *testing.T, cfg Config, opts []Option) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts = append(opts,
		WithZapLogger(zapLogger, logLevel),
		WithQueryTimeout(5*time.Second))

	client, err := Open(ctx, cfg, opts...)
	require.NoError(t, err)
	defer client.Close()

	qCtx := client.QueryCtx()

	prepareUsersTable(ctx, t, qCtx)

	// make tx with 10 users
	tx, err := qCtx.Tx(ctx)
	require.NoError(t, err)
	require.NotNil(t, tx)

	var res *query.Result
	for i := 0; i < 10; i++ {
		res, err = txQuery(tx).Exec(ctx)
		verifyResult(t, res, err)
	}
	err = tx.Commit(ctx)
	require.NoError(t, err)

	assertUsers(ctx, t, qCtx, 10)

	// make and rollback tx with 10 users
	tx, err = qCtx.Tx(ctx)
	require.NoError(t, err)
	require.NotNil(t, tx)

	for i := 0; i < 10; i++ {
		res, err = txQuery(tx).Exec(ctx)
		verifyResult(t, res, err)
	}
	err = tx.Rollback(ctx)
	require.NoError(t, err)

	assertUsers(ctx, t, qCtx, 10)

	// make tx with 10 users (inline commit)
	tx, err = qCtx.Tx(ctx)
	require.NoError(t, err)
	require.NotNil(t, tx)

	for i := 0; i < 9; i++ {
		res, err = txQuery(tx).Exec(ctx)
		verifyResult(t, res, err)
	}
	res, err = txQuery(tx).Commit().Exec(ctx)
	verifyResult(t, res, err)

	assertUsers(ctx, t, qCtx, 20)

	dropUsersTable(ctx, t, qCtx)
}

func assertUsers(ctx context.Context, t *testing.T, qCtx *query.Ctx, usersCount int) {
	t.Helper()

	usrCtr := 0
	res, err := qCtx.Query("SELECT * FROM users").
		Collect(func(rows []*Ydb.Value) error {
			for range rows {
				usrCtr++
			}
			return nil
		}).Exec(ctx)
	verifyResult(t, res, err)
	assert.Equal(t, usersCount, usrCtr)
}

func txQuery(tx *query.Transaction) *query.TxQuery {
	return tx.Query(userUpsert).
		Param("$user_id", types.Uint64(gofakeit.Uint64())).
		Param("$first_name", types.UTF8(gofakeit.FirstName())).
		Param("$last_name", types.UTF8(gofakeit.LastName())).
		Param("$email", types.UTF8(gofakeit.Email())).
		Param("$registered_ts", types.Uint64(gofakeit.Uint64()))
}

func prepareUsersTable(ctx context.Context, t *testing.T, qCtx *query.Ctx) {
	t.Helper()

	res, err := qCtx.Exec(ctx, `DROP TABLE IF EXISTS users`)
	verifyResult(t, res, err)

	res, err = qCtx.Exec(ctx, usersTable)
	verifyResult(t, res, err)
}

func dropUsersTable(ctx context.Context, t *testing.T, qCtx *query.Ctx) {
	t.Helper()

	res, err := qCtx.Exec(ctx, `DROP TABLE IF EXISTS users`)
	verifyResult(t, res, err)
}

func verifyResult(t *testing.T, res *query.Result, err error) {
	t.Helper()

	require.NoError(t, err)
	require.NotNil(t, res)
	require.NoError(t, res.Err())
	require.Empty(t, res.Issues())
	require.NotNil(t, res.Stats())
}
