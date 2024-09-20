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
	}), os.Stdout, zapcore.InfoLevel))
)

func init() {
	if val, ok := os.LookupEnv("YDB_TEST_ENDPOINT"); ok {
		ydbEndpoint = val
	}
}

func TestClient_OpenClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := Open(ctx, Config{
		InitialNodes: []string{ydbEndpoint},
		DB:           ydbPath,
	}, WithZeroLogger(zeroLogger))
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

func TestClient_Queries(t *testing.T) {
	const (
		usersCount = 100
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := Open(ctx,
		Config{
			InitialNodes: []string{ydbEndpoint},
			DB:           ydbPath,
		},
		WithZeroLogger(zeroLogger),
		WithQueryTimeout(5*time.Second),
	)
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

func TestClient_TransactionFinished(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := Open(ctx,
		Config{
			InitialNodes: []string{ydbEndpoint},
			DB:           ydbPath,
		},
		WithZeroLogger(zeroLogger),
		WithQueryTimeout(5*time.Second),
	)
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

func TestClient_Transactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := Open(ctx,
		Config{
			InitialNodes: []string{ydbEndpoint},
			DB:           ydbPath,
		},
		WithZapLogger(zapLogger),
		WithQueryTimeout(5*time.Second),
	)
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
	res, err := qCtx.Query("SELECT * FROM users").Collect(func(rows []*Ydb.Value) error {
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
