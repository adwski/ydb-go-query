## Overview

Experimental [YDB](https://ydb.tech) client that focuses on query service.

```go
import ydb "github.com/adwski/ydb-go-query/v1"

client, _ := ydb.Open(ctx, ydb.Config{
    InitialNodes: []string{"4.3.2.1:2136"}, // endpoints used for discovery
    DB:           "/mydb",             // database path
}

res, err := client.QueryCtx().Exec(ctx, `SELECT 1`)

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
```

Features available at the moment:

- Query execution with and without transactions
- Multiple transaction modes
- Client-side 3-levels load balancing (dc->node->connection) with continuous 'out-of-band' nodes discovery
- Session pool with session recycling
- Yandex Cloud IAM auth (for serverless YDB) and user-pass auth

TODO:

- DC/location priorities for balancer
- Migrations
- Scripts
- More type helpers

Work in progress

This project does not aim to replace [official SDK](https://github.com/ydb-platform/ydb-go-sdk).

## Examples

Can be found in `examples/`
