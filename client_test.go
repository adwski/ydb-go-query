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
)

var (
	ydbEndpoint = "127.0.0.1:2136"

	zeroLogger = zerolog.New(zerolog.NewConsoleWriter()).
			Level(zerolog.DebugLevel).
			With().Timestamp().Logger()
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

	res, err := qCtx.Exec(ctx, `DROP TABLE IF EXISTS users`)
	verifyResult(t, res, err)

	res, err = qCtx.Exec(ctx, usersTable)
	verifyResult(t, res, err)

	for i := 0; i < usersCount; i++ {
		res, err = qCtx.Query(`DECLARE $user_id AS Uint64;
			DECLARE $first_name AS Utf8;
			DECLARE $last_name AS Utf8;
			DECLARE $email AS Utf8;
			DECLARE $registered_ts AS Uint64;
			UPSERT INTO users (
				user_id, first_name, last_name, email, registered_ts
			) VALUES (
				$user_id, $first_name, $last_name, $email, $registered_ts)`).
			Param("$user_id", types.Uint64(gofakeit.Uint64())).
			Param("$first_name", types.UTF8(gofakeit.FirstName())).
			Param("$last_name", types.UTF8(gofakeit.LastName())).
			Param("$email", types.UTF8(gofakeit.Email())).
			Param("$registered_ts", types.Uint64(gofakeit.Uint64())).Exec(ctx)
		verifyResult(t, res, err)
	}

	selectQ := func(t *testing.T, qCtx *query.Ctx) {
		t.Helper()

		usrCtr := 0
		res, err = qCtx.Query("SELECT * FROM users").Collect(func(rows []*Ydb.Value) error {
			for _, _ = range rows {
				usrCtr++
			}
			return nil
		}).Exec(ctx)
		verifyResult(t, res, err)
		assert.Equal(t, usersCount, usrCtr)
	}

	// try different tx modes
	selectQ(t, qCtx)
	selectQ(t, qCtx.SnapshotReadOnly())
	selectQ(t, qCtx.OnlineReadOnly())
	selectQ(t, qCtx.OnlineReadOnlyInconsistent())
	selectQ(t, qCtx.StaleReadOnly())

	res, err = qCtx.Exec(ctx, `DROP TABLE IF EXISTS users`)
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
