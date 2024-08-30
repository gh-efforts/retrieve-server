package main

import (
	"fmt"

	"github.com/gh-efforts/retrieve-server/db"
	"github.com/urfave/cli/v2"
)

var migrateCmd = &cli.Command{
	Name:      "migrate",
	Usage:     "<sqlite-db> <yugabyte-dsn>",
	UsageText: "migrate sqlite db to yugabyte db",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "debug",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		setLog(cctx.Bool("debug"))

		if cctx.Args().Len() < 2 {
			return fmt.Errorf("args < 2")
		}

		return db.MergeSQLiteToYugabyte(cctx.Args().Get(0), cctx.Args().Get(1))
	},
}
