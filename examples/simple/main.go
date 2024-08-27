package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	ydb "github.com/adwski/ydb-go-query/v1"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
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
		// configure secure connection params
		// by default it's without TLS and no auth
		options = append(options,
			ydb.WithTransportTLS(),
			ydb.WithYCAuthFile(*ycIamFile))
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

	logger.Info().Msg("running query")

	// exec query
	result, err := client.Query().Exec(ctx, "select 1;", nil)
	if err != nil {
		logger.Error().Err(err).Msg("query failed")
	} else {
		fmt.Printf("==========\nGot query result:\nstats: %v\ncols: %v\n",
			result.Stats(), result.Cols())
		for rIdx, row := range result.Rows() {
			fmt.Printf("row %d: %v\n", rIdx, row)
		}
		fmt.Println("==========")
	}

	// close client
	client.Close() // blocks until all cleanup is finished
}
