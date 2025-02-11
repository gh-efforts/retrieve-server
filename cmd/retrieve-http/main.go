package main

import (
	"compress/gzip"
	"fmt"
	"math/rand/v2"
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
		&cli.Float64Flag{
			Name:  "reject-rate",
			Value: 0.0,
			Usage: "Random rejection rate (0.0-1.0)",
		},
		&cli.StringFlag{
			Name:     "car-info",
			Required: true,
			Value:    "./car-info",
			Usage:    "car info file path",
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

		if err := loadCarInfo(cctx.String("car-info")); err != nil {
			return fmt.Errorf("load car info failed: %w", err)
		}

		http.Handle("/metrics", exporter)

		http.HandleFunc("/reload", func(w http.ResponseWriter, r *http.Request) {
			if err := loadCarInfo(cctx.String("car-info")); err != nil {
				log.Errorw("reload car info failed", "error", err)
				http.Error(w, fmt.Sprintf("reload failed: %s", err), http.StatusInternalServerError)
				return
			}

			log.Info("reload car info success")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("reload success"))
		})

		rejectRate := cctx.Float64("reject-rate")
		if rejectRate < 0 || rejectRate > 1 {
			return fmt.Errorf("reject-rate must be between 0.0 and 1.0")
		}

		lsys := storeutil.LinkSystemForBlockstore(client.New(cctx.String("server-addr")))
		http.Handle(
			"/ipfs/",
			logHandler(
				carHandler(
					randomRejectHandler(
						frisbii.NewHttpIpfs(ctx, lsys, frisbii.WithCompressionLevel(gzip.NoCompression)),
						rejectRate,
					),
				),
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
		log.Debugw("incoming request",
			"url", r.URL.String(),
			"method", r.Method,
			"remote_addr", r.RemoteAddr,
			"X-Real-IP", r.Header.Get("X-Real-IP"),
			"X-Forwarded-For", r.Header.Get("X-Forwarded-For"),
		)

		next.ServeHTTP(w, r)
	})
}

func randomRejectHandler(next http.Handler, rejectRate float64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rand.Float64() < rejectRate {
			log.Debugw("random reject",
				"path", r.URL.Path,
				"method", r.Method,
				"reject_rate", rejectRate,
			)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		log.Debugw("retrieve server",
			"path", r.URL.Path,
			"method", r.Method,
		)
		next.ServeHTTP(w, r)
	})
}

func carHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dagScope := r.URL.Query().Get("dag-scope")
		if dagScope == "all" || dagScope == "" {
			log.Debugw("car handler",
				"path", r.URL.Path,
				"method", r.Method,
			)
			car(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
