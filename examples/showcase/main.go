package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	ydb "github.com/adwski/ydb-go-query/v1"
	"github.com/adwski/ydb-go-query/v1/query"
	"github.com/adwski/ydb-go-query/v1/types"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func main() {
	// create logger
	logger := zerolog.New(zerolog.NewConsoleWriter()).
		Level(zerolog.TraceLevel).
		With().Timestamp().Logger()

	// parse command line args
	var (
		fs        = pflag.NewFlagSet("", pflag.ContinueOnError)
		addr      = fs.StringP("address", "a", "127.0.0.1:12136", "YDB server address")
		db        = fs.StringP("database", "d", "/local", "YDB database path")
		poolSize  = fs.UintP("pool-size", "p", 10, "YDB session pool size")
		ycIamFile = fs.StringP("yc-iam-key-file", "y", "", "Yandex Cloud IAM key file")
		logLevel  = fs.StringP("log-level", "l", "error", "Log level: error|info|debug|trace")
	)

	err := fs.Parse(os.Args[1:])
	if err != nil {
		logger.Error().Err(err).Msg("cannot parse command line args")
		os.Exit(1)
	}

	lvl, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		logger.Error().Err(err).Msg("incorrect log level")
		os.Exit(1)
	}
	logger = logger.Level(lvl)

	// configure ydb client
	options := []ydb.Option{
		ydb.WithZeroLogger(logger.With().Str("component", "ydb").Logger()),
		ydb.WithSessionPoolSize(*poolSize)}

	if *ycIamFile != "" {
		// Enable Yandex Cloud authentication & authorization.
		options = append(options,
			ydb.WithTransportTLS(),         // use TLS
			ydb.WithYCAuthFile(*ycIamFile)) // use YC authorization with service acc IAM key
	}

	// create run context
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// create client
	client, err := ydb.Open(ctx, ydb.Config{
		InitialNodes: []string{*addr}, // address:port endpoints used for discovery
		DB:           *db,             // database path
	}, options...)
	if err != nil {
		logger.Error().Err(err).Msg("cannot create ydb client")
		defer os.Exit(1)
		return
	}
	defer client.Close() // blocks until all cleanup is finished

	// ----------------------------------------------------------------------
	// Run queries

	// client.QueryCtx() returns query execution context which holds
	// global configuration that all queries use.
	// At the moment it controls transaction mode only.
	qCtx := client.QueryCtx()

	// ----------------------------------------------------------------------
	// Simple query execution.

	// Exec() executes query outside of transaction
	// and with no parameters.
	// It blocks until it fetches result completely.
	// It also provides basic execution stats.
	checkResult(qCtx.Exec(ctx, `SELECT 1`))

	checkResult(qCtx.Exec(ctx, seriesCreateTable))
	checkResult(qCtx.Exec(ctx, seasonsCreateTable))
	checkResult(qCtx.Exec(ctx, episodesCreateTable))

	// ----------------------------------------------------------------------
	// Execute several queries inside transaction.

	// Tx() creates 'transaction' entity which allows to
	// execute several queries in one transaction.
	// Under the hood it will acquire and hold YDB session until transaction is finished.
	// Default tx mode is serializable read/write.
	// Read more about transactions here https://ydb.tech/docs/en/concepts/transactions.
	tx, errTx := qCtx.Tx(ctx)
	if errTx != nil {
		logger.Error().Err(err).Msg("cannot create transaction")
		defer os.Exit(1)
		return
	}
	// exec queries inside this transaction
	// Query() creates 'query' entity which can be configured
	// and then executed with Exec(ctx).
	checkResult(tx.Query(seasonsData).Exec(ctx))
	checkResult(tx.Query(seriesData).Exec(ctx))
	// Commit() will configure query to send inline commit flag.
	// Only last query in transaction should have commit flag set to true.
	// This flag also implies transaction cleanup (underlying session will be freed).
	checkResult(tx.Query(episodesData).Commit().Exec(ctx))

	/*
		// Or you can also commit transaction with explicit tx.Commit() call
		// which will result in +1 request to YDB.
		checkResult(tx.QueryCtx(episodesData).Exec(ctx)) // no commit flag
		if err = tx.Commit(ctx); err != nil {
			logger.Error().Err(err).Msg("cannot commit transaction")
			defer os.Exit(1)
			return
		}
	*/
	// After transaction is commited any following calls to
	// tx.QueryCtx().Exec(), tx.Rollback() or tx.Commit() will result in error.

	// ----------------------------------------------------------------------
	// Select with params

	// qCtx.Query() also uses transaction settings which means
	// every call will be executed in its own transaction.
	checkResult(qCtx.Query(
		`DECLARE $seriesId AS Uint64;
	SELECT
	    sa.title AS season_title,
	    sr.title AS series_title,
	    sr.series_id,
	    sa.season_id
	FROM
	    seasons AS sa
	INNER JOIN
	    series AS sr
	ON sa.series_id = sr.series_id
	WHERE sa.series_id = $seriesId
	ORDER BY sr.series_id, sa.season_id`).
		Param("$seriesId", types.Uint64(1)). // Parameters for query declared with DECLARE
		Exec(ctx))

	checkResult(qCtx.Query(`UPSERT INTO episodes (
		series_id, season_id, episode_id, title, air_date
	) VALUES (
    2, 5, 13, "Test Episode", CAST(Date("2018-08-27") AS Uint64))`).Exec(ctx))

	selectEpisodes := func() {
		checkResult(qCtx.Query(`DECLARE $seriesId AS Uint64;
	DECLARE $seasonId AS Uint64;
	SELECT * FROM episodes WHERE series_id = $seriesId AND season_id = $seasonId;`).
			Param("$seriesId", types.Uint64(2)).
			Param("$seasonId", types.Uint64(5)).

			// Using user-defined function for result row collection.
			// This func will be called every time new result part is arrived.
			// If nil, rows are collected internally and can be retrieved by result.Rows().
			CollectRows(
				func(rows []*Ydb.Value) error {
					fmt.Println(">>> Collecting rows in user defined function.")
					for _, row := range rows {
						fmt.Print("row: ")
						for idx, col := range row.Items {
							fmt.Printf("col%d: %v ", idx, col.String())
						}
						fmt.Println()
					}

					// If this func returns error result parts collection will stop
					// and result stream will be canceled.
					return nil
				}).Exec(ctx))
	}
	selectEpisodes()

	// ----------------------------------------------------------------------
	// Delete with params

	checkResult(qCtx.Query(`DECLARE $title AS Utf8;
	DELETE FROM episodes WHERE title = $title;`).
		Param("$title", types.UTF8("Test Episode")).
		Exec(ctx))
	selectEpisodes()

	// ----------------------------------------------------------------------
	// Create and rollback transaction.

	if tx, err = qCtx.Tx(ctx); err != nil {
		logger.Error().Err(err).Msg("cannot create transaction")
		defer os.Exit(1)
		return
	}
	checkResult(tx.Query(`UPSERT INTO episodes (
		series_id, season_id, episode_id, title, air_date
	) VALUES (
    2, 5, 13, "Test Episode", CAST(Date("2018-08-27") AS Uint64))`).Exec(ctx))

	// Only uncommitted transaction can be rolled back.
	if err = tx.Rollback(ctx); err != nil {
		logger.Error().Err(err).Msg("cannot rollback transaction")
		defer os.Exit(1)
		return
	}
	selectEpisodes()

	// ----------------------------------------------------------------------
	// cleanup

	checkResult(qCtx.Exec(ctx, `ALTER TABLE episodes ADD COLUMN viewers Uint64;`))
	checkResult(qCtx.Exec(ctx, `ALTER TABLE episodes DROP COLUMN viewers;`))
	checkResult(qCtx.Exec(ctx, `DROP TABLE series`))
	checkResult(qCtx.Exec(ctx, `DROP TABLE seasons`))
	checkResult(qCtx.Exec(ctx, `DROP TABLE episodes`))

}

func checkResult(result *query.Result, err error) {
	fmt.Println("==============================")
	// check result
	switch {
	case err != nil:
		// ydb interaction error (most likely io error)
		fmt.Printf("YDB error: %v\n", err)

	case result.Err() != nil:
		// query execution error (syntax, etc...)
		fmt.Printf("Query returned error: %v\nIssues: \n%v\n",
			result.Err(), result.Issues())

	default:
		// all fine here
		fmt.Printf("Query executed successfully!\nstats: %v\ncols: %v\n",
			result.Stats(), result.Cols())
		for rIdx, row := range result.Rows() {
			fmt.Printf("row %d: %v\n", rIdx, row)
		}
	}
	fmt.Println("==============================")
}
