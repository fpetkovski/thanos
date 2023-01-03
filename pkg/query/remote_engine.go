package query

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-community/promql-engine/api"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

type Client struct {
	maxt        int64
	labelSets   []labels.Labels
	queryClient querypb.QueryClient
}

type remoteEndpoints struct {
	getClients    func() []Client
	replicaLabels []string
	timeout       time.Duration
}

func NewRemoteEndpoints(getClients func() []Client, replicaLabels []string, timeout time.Duration) api.RemoteEndpoints {
	return remoteEndpoints{
		replicaLabels: replicaLabels,
		getClients:    getClients,
		timeout:       timeout,
	}
}

func (r remoteEndpoints) Engines() []api.RemoteEngine {
	clients := r.getClients()
	engines := make([]api.RemoteEngine, len(clients))
	for i := range clients {
		engines[i] = newRemoteEngine(clients[i].maxt, clients[i].labelSets, clients[i].queryClient, r.replicaLabels, r.timeout)
	}
	return engines
}

type remoteEngine struct {
	maxt          int64
	labelSets     []labels.Labels
	queryClient   querypb.QueryClient
	replicaLabels []string
	timeout       time.Duration
}

func newRemoteEngine(maxt int64, labelSets []labels.Labels, queryClient querypb.QueryClient, replicaLabels []string, timeout time.Duration) api.RemoteEngine {
	return &remoteEngine{
		maxt:          maxt,
		labelSets:     labelSets,
		queryClient:   queryClient,
		replicaLabels: replicaLabels,
		timeout:       timeout,
	}
}

func (r remoteEngine) MaxT() int64 {
	return r.maxt
}

func (r remoteEngine) LabelSets() []labels.Labels {
	replicaLabelSet := make(map[string]struct{})
	for _, lbl := range r.replicaLabels {
		replicaLabelSet[lbl] = struct{}{}
	}

	// Strip replica labels from the result.
	result := make([]labels.Labels, len(r.labelSets))
	for i := range r.labelSets {
		numLabels := len(r.labelSets[i]) - len(r.replicaLabels)
		if numLabels < 0 {
			continue
		}
		result[i] = make(labels.Labels, 0, numLabels)
		for _, lbl := range r.labelSets[i] {
			if _, ok := replicaLabelSet[lbl.Name]; ok {
				continue
			}
			result[i] = append(result[i], lbl)
		}
	}

	return result
}

func (r remoteEngine) NewRangeQuery(opts *promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return &remoteQuery{
		replicaLabels: r.replicaLabels,
		timeout:       r.timeout,
		client:        r.queryClient,
		qs:            qs,
		start:         start,
		end:           end,
		interval:      interval,
	}, nil
}

type remoteQuery struct {
	client        querypb.QueryClient
	replicaLabels []string
	timeout       time.Duration

	qs       string
	start    time.Time
	end      time.Time
	interval time.Duration

	cancel context.CancelFunc
}

func (r *remoteQuery) Exec(ctx context.Context) *promql.Result {
	qctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	defer cancel()

	request := &querypb.QueryRangeRequest{
		Query:            r.qs,
		StartTimeSeconds: r.start.Unix(),
		EndTimeSeconds:   r.end.Unix(),
		IntervalSeconds:  int64(r.interval.Seconds()),
		TimeoutSeconds:   int64(r.timeout.Seconds()),
		// TODO (fpetkovski): Allow specifying these parameters at query time.
		ReplicaLabels: r.replicaLabels,
		EnableDedup:   true,
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
			Metric: labelpb.ZLabelsToPromLabels(ts.Labels),
			Points: make([]promql.Point, 0, len(ts.Samples)),
		}
		for _, s := range ts.Samples {
			series.Points = append(series.Points, promql.Point{
				T: s.Timestamp,
				V: s.Value,
			})
		}
		result = append(result, series)
	}

	return &promql.Result{Value: result}
}

func (r *remoteQuery) Close() {
	r.Cancel()
}

func (r *remoteQuery) Statement() parser.Statement {
	return nil
}

func (r *remoteQuery) Stats() *stats.Statistics {
	return nil
}

func (r *remoteQuery) Cancel() {
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *remoteQuery) String() string {
	return r.qs
}
