[![Go Report Card](https://goreportcard.com/badge/github.com/adwski/ydb-go-query)](https://goreportcard.com/report/github.com/adwski/ydb-go-query)
[![CI](https://github.com/adwski/ydb-go-query/actions/workflows/tests.yml/badge.svg)](https://github.com/adwski/ydb-go-query/actions/workflows/tests.yml)
![Coverage](https://img.shields.io/badge/Coverage-18.5%25-red)

# ydb-go-query

Experimental [YDB](https://ydb.tech) client that focuses on query service. Work in progress.


## Features

- Query execution (with parameters)
- Transactions
- Client-side 3-levels load balancing (dc->node->connection) with continuous 'out-of-band' nodes discovery
- Session pool with session recycling
- Yandex Cloud IAM auth (for serverless YDB) and user-pass auth

### TODO

- [ ] DC/location priorities for balancer
- [ ] Migrations
- [ ] Scripts
- [ ] More type helpers
- [ ] Retries

## Usage

```go
package main

import (
	"context"
	"fmt"
	ydb "github.com/adwski/ydb-go-query"
)

func main() {
	ctx := context.Background()

	client, _ := ydb.Open(ctx, ydb.Config{
		InitialNodes: []string{"4.3.2.1:2136"}, // endpoints used for discovery
		DB:           "/mydb",                  // database path
	})

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

More examples can be found in `examples/`.
