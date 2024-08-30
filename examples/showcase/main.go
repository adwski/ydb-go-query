package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	ydb "github.com/adwski/ydb-go-query/v1"
	"github.com/adwski/ydb-go-query/v1/query/result"

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

	// create run context
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

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

	// run queries

	// Exec() executes query outside of transaction.
	// It blocks until it fetches result completely.
	// It also provides basic execution stats.
	checkResult(client.Query().Exec(ctx, `SELECT 1`, nil))

	// ExecDDL() is same as Exec() but doesn't have params.
	checkResult(client.Query().ExecDDL(ctx, seriesCreateTable))
	checkResult(client.Query().ExecDDL(ctx, seasonsCreateTable))
	checkResult(client.Query().ExecDDL(ctx, episodesCreateTable))

	// Tx() creates 'transaction settings scope'.
	// This means that every ExecOne() will be executed in its own transaction.
	// Every transaction inherits settings of the scope, i.e. transaction mode.
	// By default, tx mode is serializable read/write.
	// Read more about transactions here https://ydb.tech/docs/en/concepts/transactions.
	checkResult(client.Query().Tx().ExecOne(ctx, seriesData, nil, nil))

	// ExecMany allows to execute several queries in one transaction.
	// This approach requires explicit transaction creation (which can lead to error).
	tx, errTx := client.Query().Tx().ExecMany(ctx)
	if errTx != nil {
		logger.Error().Err(err).Msg("cannot create transaction")
		defer os.Exit(1)
		return
	}
	// exec query with this transaction
	checkResult(tx.Do(ctx, seasonsData, nil, nil, false))
	// Only last query in transaction should have commit flag set to true.
	// This flag also implies transaction cleanup (underlying session will be freed).
	checkResult(tx.Do(ctx, episodesData, nil, nil, true))
	// Any following calls to tx.Do() will result in error.

	// Here tx mode is online-read-only.
	checkResult(client.Query().Tx().OnlineReadOnly().ExecOne(ctx, `DECLARE $seriesId AS Uint64;
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
	ORDER BY
	    sr.series_id,
	    sa.season_id`,
		map[string]*Ydb.TypedValue{
			"$seriesId": {
				Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}},
				Value: &Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: 1}},
			},
		}, nil))

	checkResult(client.Query().Tx().ExecOne(ctx, `UPSERT INTO episodes (
		series_id, season_id, episode_id, title, air_date
	) VALUES (
    2, 5, 13, "Test Episode", CAST(Date("2018-08-27") AS Uint64))`, nil, nil))

	selectEpisodes := func() {
		checkResult(client.Query().Tx().ExecOne(ctx, `DECLARE $seriesId AS Uint64; DECLARE $seasonId AS Uint64;
	SELECT * FROM episodes WHERE series_id = $seriesId AND season_id = $seasonId;`,
			map[string]*Ydb.TypedValue{
				"$seriesId": {
					Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}},
					Value: &Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: 2}},
				},
				"$seasonId": {
					Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}},
					Value: &Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: 5}},
				},
			},
			// This func will be called every time new result part is arrived.
			// If nil, rows are collected internally and can be retrieved by result.Rows().
			func(rows []*Ydb.Value) {
				fmt.Println(">>> Collecting rows in user defined function.")
				for _, row := range rows {
					fmt.Print("row: ")
					for idx, col := range row.Items {
						fmt.Printf("col%d: %v ", idx, col.String())
					}
					fmt.Println()
				}
			}))
	}
	selectEpisodes()

	checkResult(client.Query().Tx().ExecOne(ctx, `DECLARE $title AS Utf8;
	DELETE FROM episodes WHERE title = $title;`,
		map[string]*Ydb.TypedValue{
			"$title": {
				Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
				Value: &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: "Test Episode"}},
			},
		}, nil))
	selectEpisodes()

	checkResult(client.Query().Exec(ctx, `ALTER TABLE episodes ADD COLUMN viewers Uint64;`, nil))
	checkResult(client.Query().Exec(ctx, `ALTER TABLE episodes DROP COLUMN viewers;`, nil))
	checkResult(client.Query().Exec(ctx, `DROP TABLE series`, nil))
	checkResult(client.Query().Exec(ctx, `DROP TABLE seasons`, nil))
	checkResult(client.Query().Exec(ctx, `DROP TABLE episodes`, nil))

	// close client
	client.Close() // blocks until all cleanup is finished
}

func checkResult(result *result.Result, err error) {
	fmt.Println("==========")
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
	fmt.Println("==========")
}
