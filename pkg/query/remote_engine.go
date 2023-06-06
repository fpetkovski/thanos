// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"io"
	"time"

	"github.com/efficientgo/core/backoff"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-io/promql-engine/api"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// Opts are the options for a PromQL query.
type Opts struct {
	AutoDownsample        bool
	ReplicaLabels         []string
	Timeout               time.Duration
	EnablePartialResponse bool
}

// Client is a query client that executes PromQL queries.
type Client struct {
	querypb.QueryClient
	address   string
	mint      int64
	maxt      int64
	labelSets []labels.Labels
}

// NewClient creates a new Client.
func NewClient(queryClient querypb.QueryClient, address string, mint int64, maxt int64, labelSets []labels.Labels) Client {
	return Client{QueryClient: queryClient, address: address, mint: mint, maxt: maxt, labelSets: labelSets}
}

func (q Client) MinT() int64 { return q.mint }
func (q Client) MaxT() int64 { return q.maxt }

func (q Client) LabelSets() []labels.Labels { return q.labelSets }

func (q Client) GetAddress() string { return q.address }

type remoteEndpoints struct {
	logger     log.Logger
	getClients func() []Client
	opts       Opts
}

func NewRemoteEndpoints(logger log.Logger, getClients func() []Client, opts Opts) api.RemoteEndpoints {
	return remoteEndpoints{
		logger:     logger,
		getClients: getClients,
		opts:       opts,
	}
}

func (r remoteEndpoints) Engines() []api.RemoteEngine {
	clients := r.getClients()
	engines := make([]api.RemoteEngine, len(clients))
	for i := range clients {
		engines[i] = NewRemoteEngine(r.logger, clients[i], r.opts)
	}
	return engines
}

type remoteEngine struct {
	Client
	opts   Opts
	logger log.Logger
}

func NewRemoteEngine(logger log.Logger, queryClient Client, opts Opts) *remoteEngine {
	return &remoteEngine{
		logger: logger,
		Client: queryClient,
		opts:   opts,
	}
}

func (r remoteEngine) LabelSets() []labels.Labels {
	replicaLabelSet := make(map[string]struct{})
	for _, lbl := range r.opts.ReplicaLabels {
		replicaLabelSet[lbl] = struct{}{}
	}

	// Strip replica labels from the result.
	labelSets := r.Client.LabelSets()
	result := make([]labels.Labels, 0, len(labelSets))
	for _, labelSet := range labelSets {
		var builder labels.ScratchBuilder
		for _, lbl := range labelSet {
			if _, ok := replicaLabelSet[lbl.Name]; ok {
				continue
			}
			builder.Add(lbl.Name, lbl.Value)
		}
		result = append(result, builder.Labels())
	}

	return result
}

func (r remoteEngine) NewRangeQuery(_ context.Context, opts *promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	qry := &remoteQuery{
		logger: r.logger,
		client: r.Client,
		opts:   r.opts,

		qs:       qs,
		start:    start,
		end:      end,
		interval: interval,
	}

	return newRetriableQuery(qry), nil
}

func (r remoteEngine) NewInstantQuery(_ context.Context, _ *promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	qry := &remoteQuery{
		logger: r.logger,
		client: r.Client,
		opts:   r.opts,

		qs:       qs,
		start:    ts,
		end:      ts,
		interval: 0,
	}

	return newRetriableQuery(qry), nil
}

type remoteQuery struct {
	logger log.Logger
	client Client
	opts   Opts

	qs       string
	start    time.Time
	end      time.Time
	interval time.Duration

	cancel context.CancelFunc
}

func (r *remoteQuery) Exec(ctx context.Context) *promql.Result {
	start := time.Now()

	qctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	defer cancel()

	var maxResolution int64
	if r.opts.AutoDownsample {
		maxResolution = int64(r.interval.Seconds() / 5)
	}

	// Instant query.
	if r.start == r.end {
		request := &querypb.QueryRequest{
			Query:                 r.qs,
			TimeSeconds:           r.start.Unix(),
			TimeoutSeconds:        int64(r.opts.Timeout.Seconds()),
			EnablePartialResponse: r.opts.EnablePartialResponse,
			// TODO (fpetkovski): Allow specifying these parameters at query time.
			// This will likely require a change in the remote engine interface.
			ReplicaLabels:        r.opts.ReplicaLabels,
			MaxResolutionSeconds: maxResolution,
			EnableDedup:          true,
		}

		qry, err := r.client.Query(qctx, request)
		if err != nil {
			return &promql.Result{Err: err}
		}
		result := make(promql.Vector, 0)

		for {
			msg, err := qry.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return &promql.Result{Err: err}
			}

			if warn := msg.GetWarnings(); warn != "" {
				return &promql.Result{Err: errors.New(warn)}
			}

			ts := msg.GetTimeseries()

			// Point might have a different timestamp, force it to the evaluation
			// timestamp as that is when we ran the evaluation.
			// See https://github.com/prometheus/prometheus/blob/b727e69b7601b069ded5c34348dca41b80988f4b/promql/engine.go#L693-L699
			if len(ts.Histograms) > 0 {
				result = append(result, promql.Sample{Metric: labelpb.ZLabelsToPromLabels(ts.Labels), H: fromProtoHistogram(ts.Histograms[0]), T: r.start.UnixMilli()})
			} else {
				result = append(result, promql.Sample{Metric: labelpb.ZLabelsToPromLabels(ts.Labels), F: ts.Samples[0].Value, T: r.start.UnixMilli()})
			}
		}

		return &promql.Result{Value: result}
	}

	request := &querypb.QueryRangeRequest{
		Query:                 r.qs,
		StartTimeSeconds:      r.start.Unix(),
		EndTimeSeconds:        r.end.Unix(),
		IntervalSeconds:       int64(r.interval.Seconds()),
		TimeoutSeconds:        int64(r.opts.Timeout.Seconds()),
		EnablePartialResponse: r.opts.EnablePartialResponse,
		// TODO (fpetkovski): Allow specifying these parameters at query time.
		// This will likely require a change in the remote engine interface.
		ReplicaLabels:        r.opts.ReplicaLabels,
		MaxResolutionSeconds: maxResolution,
		EnableDedup:          true,
	}
	qry, err := r.client.QueryRange(qctx, request)
	if err != nil {
		return &promql.Result{Err: err}
	}

	result := make(promql.Matrix, 0)
	for {
		msg, err := qry.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return &promql.Result{Err: err}
		}

		if warn := msg.GetWarnings(); warn != "" {
			return &promql.Result{Err: errors.New(warn)}
		}

		ts := msg.GetTimeseries()
		if ts == nil {
			continue
		}
		series := promql.Series{
			Metric:     labelpb.ZLabelsToPromLabels(ts.Labels),
			Floats:     make([]promql.FPoint, 0, len(ts.Samples)),
			Histograms: make([]promql.HPoint, 0, len(ts.Histograms)),
		}
		for _, s := range ts.Samples {
			series.Floats = append(series.Floats, promql.FPoint{
				T: s.Timestamp,
				F: s.Value,
			})
		}
		for _, hp := range ts.Histograms {
			series.Histograms = append(series.Histograms, promql.HPoint{
				T: hp.Timestamp,
				H: fromProtoHistogram(hp),
			})
		}
		result = append(result, series)
	}
	level.Debug(r.logger).Log("msg", "Executed query", "query", r.qs, "time", time.Since(start), "client", r.client.GetAddress())

	return &promql.Result{Value: result}
}

func (r *remoteQuery) Close() { r.Cancel() }

func (r *remoteQuery) Statement() parser.Statement { return nil }

func (r *remoteQuery) Stats() *stats.Statistics { return nil }

func (r *remoteQuery) Cancel() {
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *remoteQuery) String() string { return r.qs }

type retriableQuery struct {
	*remoteQuery
}

func newRetriableQuery(remoteQuery *remoteQuery) *retriableQuery {
	return &retriableQuery{remoteQuery: remoteQuery}
}

func (q *retriableQuery) Exec(ctx context.Context) *promql.Result {
	retries := backoff.New(ctx, backoff.Config{
		Min:        100 * time.Millisecond,
		Max:        1 * time.Second,
		MaxRetries: 3,
	})

	var result *promql.Result
	for retries.Ongoing() {
		result = q.remoteQuery.Exec(ctx)
		if result.Err == nil {
			return result
		}
		retries.Wait()
	}

	return result
}

func fromProtoHistogram(hp prompb.Histogram) *histogram.FloatHistogram {
	if hp.IsFloatHistogram() {
		return prompb.FloatHistogramProtoToFloatHistogram(hp)
	} else {
		return prompb.HistogramProtoToFloatHistogram(hp)
	}
}
