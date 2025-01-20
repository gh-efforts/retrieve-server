package main

import (
	"compress/gzip"
	"net/http"
	"os"
	"time"

	"contrib.go.opencensus.io/exporter/prometheus"
	"github.com/gh-efforts/retrieve-server/build"
	"github.com/gh-efforts/retrieve-server/client"
	"github.com/gh-efforts/retrieve-server/metrics"
	"github.com/ipld/frisbii"

	"github.com/filecoin-project/boost-graphsync/storeutil"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var log = logging.Logger("main")

func main() {
	local := []*cli.Command{
		runCmd,
	}

	app := &cli.App{
		Name:     "retrieve-http",
		Usage:    "retrieve http ",
		Version:  build.UserVersion(),
		Commands: local,
	}

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
	}
}

var runCmd = &cli.Command{
	Name: "run",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Value: "0.0.0.0:9875",
		},
		&cli.BoolFlag{
			Name:  "debug",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "server-addr",
			Value: "127.0.0.1:9876",
		},
	},
	Action: func(cctx *cli.Context) error {
		setLog(cctx.Bool("debug"))

		log.Info("starting retrieve http ...")

		ctx := cliutil.ReqContext(cctx)

		exporter, err := prometheus.NewExporter(prometheus.Options{
			Namespace: "rhttp",
		})
		if err != nil {
			return err
		}

		ctx, _ = tag.New(ctx,
			tag.Insert(metrics.Version, build.BuildVersion),
			tag.Insert(metrics.Commit, build.CurrentCommit),
		)
		if err := view.Register(
			metrics.Views...,
		); err != nil {
			return err
		}
		stats.Record(ctx, metrics.Info.M(1))

		listen := cctx.String("listen")
		log.Infow("retrieve http", "listen", listen)

		http.Handle("/metrics", exporter)

		lsys := storeutil.LinkSystemForBlockstore(client.New(cctx.String("server-addr")))
		http.Handle(
			"/ipfs/",
			logHandler(
				frisbii.NewHttpIpfs(ctx, lsys, frisbii.WithCompressionLevel(gzip.NoCompression)),
			),
		)

		server := &http.Server{
			Addr: listen,
		}

		go func() {
			<-ctx.Done()
			time.Sleep(time.Millisecond * 100)
			log.Info("closed retrieve http")
			server.Shutdown(ctx)
		}()

		return server.ListenAndServe()
	},
}

func setLog(debug bool) {
	level := "INFO"
	if debug {
		level = "DEBUG"
	}

	logging.SetLogLevel("main", level)
	logging.SetLogLevel("client", level)
}

func logHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 获取真实IP，考虑X-Forwarded-For和X-Real-IP头
		ip := r.Header.Get("X-Real-IP")
		if ip == "" {
			ip = r.Header.Get("X-Forwarded-For")
			if ip == "" {
				ip = r.RemoteAddr
			}
		}

		log.Debugw("incoming request",
			"path", r.URL.Path,
			"method", r.Method,
			"source_ip", ip,
		)

		next.ServeHTTP(w, r)
	})
}
