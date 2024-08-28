package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	ydb "github.com/adwski/ydb-go-query/v1"

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

	if *ycIamFile != "" { // this enables Yandex Cloud authentication & authorization
		// configure secure connection params
		// by default it's without TLS and no auth
		options = append(options,
			ydb.WithTransportTLS(),         // use TLS
			ydb.WithYCAuthFile(*ycIamFile)) // use service acc IAM key
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

	// query execution helper
	doQuery := func(
		query string,
		params map[string]*Ydb.TypedValue,
	) {
		// ExecAndFetchAll() will run query and block until it fetches result completely.
		// It will also provide basic stats.
		result, errRes := client.Query().ExecAndFetchAll(ctx, query, params)

		fmt.Println("==========")
		// check result
		switch {
		case errRes != nil:
			// ydb interaction error (most likely io error)
			fmt.Printf("YDB error: %v\n", errRes)

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

	// exec queries

	// create tables
	doQuery(`DROP TABLE IF EXISTS series`, nil)
	doQuery(seriesCreateTable, nil)

	doQuery(`DROP TABLE IF EXISTS seasons`, nil)
	doQuery(seasonsCreateTable, nil)

	doQuery(`DROP TABLE IF EXISTS episodes`, nil)
	doQuery(episodesCreateTable, nil)

	doQuery(seriesData, nil)
	doQuery(seasonsData, nil)
	doQuery(episodesData, nil)

	doQuery(`DECLARE $seriesId AS Uint64;
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
		})

	doQuery(`UPSERT INTO episodes (
		series_id, season_id, episode_id, title, air_date
	) VALUES (
    2, 5, 13, "Test Episode", CAST(Date("2018-08-27") AS Uint64))`, nil)

	selectEpisodes := func() {
		doQuery(`DECLARE $seriesId AS Uint64; DECLARE $seasonId AS Uint64;
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
			})
	}
	selectEpisodes()

	doQuery(`DECLARE $title AS Utf8;
	DELETE FROM episodes WHERE title = $title;`,
		map[string]*Ydb.TypedValue{
			"$title": {
				Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
				Value: &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: "Test Episode"}},
			},
		})
	selectEpisodes()

	doQuery(`ALTER TABLE episodes ADD COLUMN viewers Uint64;`, nil)
	doQuery(`ALTER TABLE episodes DROP COLUMN viewers;`, nil)

	doQuery(`DROP TABLE series`, nil)
	doQuery(`DROP TABLE seasons`, nil)
	doQuery(`DROP TABLE episodes`, nil)

	// close client
	client.Close() // blocks until all cleanup is finished
}
