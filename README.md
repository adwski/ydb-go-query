[![CI](https://github.com/adwski/ydb-go-query/actions/workflows/tests.yml/badge.svg)](https://github.com/adwski/ydb-go-query/actions/workflows/tests.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/adwski/ydb-go-query)](https://goreportcard.com/report/github.com/adwski/ydb-go-query)
[![Coverage](https://adwski.github.io/ydb-go-query/badges/coverage.svg)](https://adwski.github.io/ydb-go-query/coverage/coverage.html)
[![Go version](https://img.shields.io/github/go-mod/go-version/adwski/ydb-go-query.svg)](https://github.com/adwski/ydb-go-query)
[![License](https://img.shields.io/badge/License-Apache_2.0-brightgreen.svg)](https://opensource.org/licenses/Apache-2.0)

# ydb-go-query

Experimental lightweight [YDB](https://ydb.tech) client that focuses on query service. Work in progress (API might change).

# Features

- Query execution
- Transactions
- Location aware load balancing with continuous 'out-of-band' nodes discovery
- Session pool with session recycling and auto warm-up
- Authentication: user-pass and Yandex Cloud IAM (for serverless YDB).
- Works with and exposes bare YDB GRPC field types `github.com/ydb-platform/ydb-go-genproto/protos/Ydb` (but provides type helpers for convenience).
- Ready status with high and low thresholds.

## TODO

- [x] Ready status
- [x] DC/location priorities for balancer
- [ ] Migrations
- [ ] Scripts
- [ ] More type helpers
- [ ] Retries
- [ ] OpenMetrics

# Quickstart

```go
package main

import (
	"context"
	"fmt"
	ydb "github.com/adwski/ydb-go-query"
)

func main() {
    ctx := context.Background()

    // connect without authentication and TSL
    client, err := ydb.Open(ctx, ydb.Config{
        InitialNodes: []string{"127.0.0.1:2136"}, // endpoints used for discovery
        DB:           "/local",                  // database path
    })
    if err != nil {
        panic(err)
    }

    defer client.Close()

    // exec query outside of transaction
    // execution blocks until connection (and session) is available
    result, err := client.QueryCtx().Exec(ctx, `SELECT 1`)

    switch {
    case err != nil:
        fmt.Printf("YDB error: %v\n", err)

    case result.Err() != nil:
        fmt.Printf("Query error: %v\nIssues: \n%v\n", result.Err(), result.Issues())

    default:
        fmt.Printf("Ok!\nstats: %v\ncols: %v\n", result.Stats(), result.Cols())
        for rIdx, row := range result.Rows() {
            fmt.Printf("row %d: %v\n", rIdx, row)
        }
    }
}
```
## More config options

```go
// init logging with external logger.
// "debug" here is an internal level (not related to zerolog)
ydb.WithZeroLogger(zerolog.New(zerolog.NewConsoleWriter()), "debug"),

// zap is also available
// WithZapLogger(zapLogger, "debug"),

// Query execution timeout, default 5 minutes. 
ydb.WithQueryTimeout(5*time.Minute),

// Session pool size, default is 10.
ydb.WithSessionPoolSize(50),

// Session pool ready Low and High thresholds.
// If amount of ready sessions hit above High threshold,
// then client.Ready() returns true.
// If amount of ready sessions hit below Low threshold,
// then client.Ready() returns false.
// Default is low=0, high=50
ydb.WithSessionPoolReadyThresholds(10, 80)

// Session create timeout, default is 3 seconds.
ydb.WithSessionCreateTimeout(5*time.Second)

// Amount of connections to create per each discovered endpoint.
// Default is 2
ydb.WithConnectionsPerEndpoint(4),

// Location preference.
// Client will use connections in first location from this list,
// if all connections in this location are not alive, it will move to next.
// If there's no alive connections in ether of these locations,
// alive connections from other discovered locations (if any) will be used.
ydb.WithLocationPreference([]string{"ru-central1-b", "ru-central1-a"})

// tx mode, serializable rw is default
ydb.WithSerializableReadWrite()
```

## Contexts

Context provided with `ydb.Open(ctx, ...)` acts as the parent context for every running component of the client.
If it is canceled, client shuts down. It is fine to cancel this context if you want to terminate your app but make sure to call `ydb.Close()` to wait for cleanup completion. It is also acceptable to just call `ydb.Close()` without canceling this context.

Context provided in `Exec(ctx)` is the query context, used internally for timeouts and grpc calls. You can also cancel this context to abort query execution.

## Use with serverless YDB in Yandex Cloud

```go
client, err := ydb.Open(ctx,
    ydb.Config{
        InitialNodes: []string{"ydb.serverless.yandexcloud.net:2135"},
        DB:           "/ru-central1/b1g22ge123t0me6ngsfg/etn4nlihce23r24fgnk32p",
    },
    ydb.WithTransportTLS(),     // use TLS
    ydb.WithYCAuthFile("/path/to/iam/key.json"), // use YC authorization
)
```

## Exec simple queries

`client.QueryCtx()` returns query execution context which holds
global configuration that all queries use.
At the moment it controls transaction mode and query timeout.

```go
client, _ := ydb.Open(ctx, ydb.Config{
    InitialNodes: []string{"127.0.0.1:2136"},
    DB:           "/local",
})

qCtx := client.QueryCtx() // get query execution context

// DDL and DML queries must be executed outside of transaction
res, err := qCtx.Exec(`CREATE TABLE users (
    user_id Uint64,
    first_name Utf8,
    last_name Utf8,
    email Utf8,
    registered_ts Uint64,
    PRIMARY KEY (user_id))`)

if err != nil {
    panic(err)
}
```

`qCtx.Query()` executes each query in single transaction. Transaction mode is derived from `query.Ctx`.
```go
res, err = qCtx.Query(`DECLARE $user_id AS Uint64;
    DECLARE $first_name AS Utf8;
    DECLARE $last_name AS Utf8;
    DECLARE $email AS Utf8;
    DECLARE $registered_ts AS Uint64;
    UPSERT INTO users (
        user_id, first_name, last_name, email, registered_ts
    ) VALUES (
        $user_id, $first_name, $last_name, $email, $registered_ts)`).
    Param("$user_id", types.Uint64(123)).
    Param("$first_name", types.UTF8("test")).
    Param("$last_name", types.UTF8("test")).
    Param("$email", types.UTF8("test@test.test")).
    Param("$registered_ts", types.Uint64(1726836887)).
    Exec(ctx)
```

`qCtx.Query()` is used for select queries as well.
```go
res, err := qCtx.Query("SELECT * FROM users").Exec(ctx)
if err != nil {
	panic(err) // io error
}
if res.Err() != nil {
	panic(res.Err()) // query error
}
for _, row := range result.Rows() {
    fmt.Printf("row: %v\n", row)
}
```

You can gather result rows with custom function provided with `Collect()`. This func will be called every time result part is arrived. `result.Rows()` will be empty in this case.
```go
res, err := qCtx.Query("SELECT * FROM users").
    Collect(func(rows []*Ydb.Value) error {
        for _, row := range rows {
            fmt.Printf("row: %v\n", row)
        }
        return nil
    }).Exec(ctx)
```

## Transactions

`Tx()` creates transaction entity which allows to
execute several queries in one transaction.
Under the hood it will acquire and hold YDB session until transaction is finished. Tx mode is inherited from `query.Ctx`.
Read more about transactions here https://ydb.tech/docs/en/concepts/transactions.
```go
tx, err := qCtx.Tx(ctx)
if errTx != nil {
    panic(err)
}
res, err = tx.Query(`DECLARE $user_id AS Uint64;
    DECLARE $first_name AS Utf8;
    DECLARE $last_name AS Utf8;
    DECLARE $email AS Utf8;
    DECLARE $registered_ts AS Uint64;
    UPSERT INTO users (
        user_id, first_name, last_name, email, registered_ts
    ) VALUES (
        $user_id, $first_name, $last_name, $email, $registered_ts)`).
Param("$user_id", types.Uint64(123)).
Param("$first_name", types.UTF8("test")).
Param("$last_name", types.UTF8("test")).
Param("$email", types.UTF8("test@test.test")).
Param("$registered_ts", types.Uint64(1726836887)).
Exec(ctx)

if err != nil {
    panic(err)
}

// more queries here
// ...

// commit transaction
err = tx.Commit(ctx)
if err != nil {
    panic(err)
}
```
You also can send an inline commit together with the last query in the current transaction. This way transaction will be committed immediately after (successful) query execution and explicit `tx.Commit()` call is not needed. This approach saves you one round trip to YDB.
```go
res, err := tx.Query("...").
    Param("$qwe", types.UTF8("test")).
    Commit().
    Exec(ctx)
// no need to call tx.Commit() after this
```
Uncommitted transactions can be rolled back with `tx.Rollback()`.

# Feedback

If you've spotted a bug or interested in some improvement feel free to open an Issue. PRs are also welcome.
