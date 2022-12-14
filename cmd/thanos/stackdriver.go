package main

import (
	"context"
	"time"

	"github.com/efficientgo/core/errors"
	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	"github.com/thanos-io/thanos/pkg/store"
)

type stackdriverStoreConfig struct {
	grpc         grpcConfig
	reqLogConfig *extflag.PathOrContent
}

func (sdConfig *stackdriverStoreConfig) registerFlags(cmd extkingpin.AppClause) {
	sdConfig.grpc.registerFlag(cmd)
	sdConfig.reqLogConfig = extkingpin.RegisterRequestLoggingFlags(cmd)
}

func registerStackdriver(app *extkingpin.App) {
	cmd := app.Command(component.Stackdriver.String(), "Stackdriver store API.")
	conf := &stackdriverStoreConfig{}
	conf.registerFlags(cmd)
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		tagOpts, grpcLogOpts, err := logging.ParsegRPCOptions("", conf.reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}
		return runStackdriverStore(g, logger, reg, nil, component.Stackdriver, conf, grpcLogOpts, tagOpts)
	})
}

func runStackdriverStore(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	comp component.Component,
	conf *stackdriverStoreConfig,
	grpcLogOpts []grpc_logging.Option,
	tagOpts []tags.Option,
) error {
	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	sdStore := store.NewStackdriverStore()
	srv := grpcserver.New(logger, reg, tracer, grpcLogOpts, tagOpts, comp, grpcProbe,
		grpcserver.WithServer(store.RegisterStoreServer(sdStore)),
		grpcserver.WithListen(conf.grpc.bindAddress),
	)

	stopDiscovery := make(chan struct{})
	shutdown := func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		close(stopDiscovery)
		srv.Shutdown(err)
	}

	g.Add(func() error {
		if err := sdStore.RefreshMetrics(context.Background()); err != nil {
			return err
		}
		return runutil.Repeat(1*time.Hour, stopDiscovery, func() error {
			return sdStore.RefreshMetrics(context.Background())
		})
	}, shutdown)

	g.Add(func() error {
		statusProber.Healthy()
		return srv.ListenAndServe()
	}, shutdown)

	return g.Run()
}
