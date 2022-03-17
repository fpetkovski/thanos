package querysharding

import (
	"context"
	"math/rand"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type query struct {
	logger log.Logger
	cancel context.CancelFunc

	queriers []querypb.QueryClient
	analysis QueryAnalysis

	qry promql.Query
	qs  string
	ts  time.Time
}

type timeSeriesInstantIterator struct {
	q querypb.Query_QueryClient
}

func (t *timeSeriesInstantIterator) NextSeries() (*prompb.TimeSeries, string, error) {
	next, err := t.q.Recv()
	return next.GetTimeseries(), next.GetWarnings(), err
}

func newQuery(logger log.Logger, analyzer *queryAnalyzer, queriers []querypb.QueryClient, qs string, ts time.Time) (*query, error) {
	analysis, err := analyzer.Analyze(qs)
	if err != nil {
		return nil, err
	}

	return &query{
		logger:   logger,
		queriers: queriers,
		analysis: analysis,
		qs:       qs,
		ts:       ts,
	}, nil
}

func (q *query) Exec(ctx context.Context) *promql.Result {
	if len(q.queriers) == 0 {
		return &promql.Result{}
	}

	queryCtx, cancel := context.WithCancel(ctx)
	q.cancel = cancel

	clients, err := q.getQueryClients(queryCtx)
	if err != nil {
		return &promql.Result{
			Err: err,
		}
	}

	f := newFanout(queryCtx, q.logger, clients)
	series, warns, err := f.exec()
	if err != nil {
		return &promql.Result{
			Err: err,
		}
	}

	vector := make(promql.Vector, len(series))
	result := promql.Result{
		Value:    vector,
		Warnings: warns,
	}
	for i, s := range series {
		var labelset labels.Labels
		for _, label := range s.Labels {
			labelset = append(labelset, labels.Label{
				Name:  label.Name,
				Value: label.Value,
			})
		}

		vector[i] = promql.Sample{
			Point: promql.Point{
				T: s.Samples[0].Timestamp,
				V: s.Samples[0].Value,
			},
			Metric: labelset,
		}
	}

	return &result
}

func (q *query) getQueryClients(ctx context.Context) ([]timeSeriesIterator, error) {
	numQueriers := len(q.queriers)

	if !q.analysis.IsShardable() {
		level.Debug(q.logger).Log("msg", "Query is not shardable, executing on a single querier", "query", q.qs)
		queryClient, err := q.queryClientForShard(ctx, nil, numQueriers)
		if err != nil {
			return nil, err
		}
		return []timeSeriesIterator{queryClient}, nil
	}

	clients := make([]timeSeriesIterator, numQueriers)
	for i := range q.queriers {
		queryClient, err := q.queryClientForShard(ctx, &storepb.ShardInfo{
			ShardIndex:  int64(i),
			TotalShards: int64(numQueriers),
			By:          q.analysis.ShardBy(),
			Labels:      q.analysis.ShardingLabels(),
		}, numQueriers)
		if err != nil {
			return nil, err
		}
		clients[i] = queryClient
	}

	return clients, nil
}

func (q *query) queryClientForShard(ctx context.Context, shard *storepb.ShardInfo, numClients int) (timeSeriesIterator, error) {
	var querierIndex int64
	if shard != nil {
		querierIndex = shard.ShardIndex
	} else {
		querierIndex = rand.Int63n(int64(numClients))
	}

	queryClient, err := q.queriers[querierIndex].Query(ctx, &querypb.QueryRequest{
		Query:          q.qs,
		TimeSeconds:    q.ts.Unix(),
		TimeoutSeconds: int64(60 * time.Second),
		EnableDedup:    true,
		ShardInfo:      shard,
	})

	if err != nil {
		return nil, err
	}

	return &timeSeriesInstantIterator{
		q: queryClient,
	}, nil
}

func (q *query) Close() {}

func (q *query) Statement() parser.Statement {
	//TODO implement me
	panic("implement me")
}

func (q *query) Stats() *stats.QueryTimers {
	//TODO implement me
	panic("implement me")
}

func (q *query) Cancel() {
	if q.cancel != nil {
		q.cancel()
	}
}

func (q *query) String() string {
	return q.qs
}
