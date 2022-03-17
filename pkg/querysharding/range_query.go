package querysharding

import (
	"context"
	"math/rand"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type rangeQuery struct {
	logger log.Logger
	cancel context.CancelFunc

	queriers []querypb.QueryClient
	analysis QueryAnalysis

	qry      promql.Query
	qs       string
	from     time.Time
	to       time.Time
	interval time.Duration
}

type timeSeriesRangeIterator struct {
	q querypb.Query_QueryRangeClient
}

func (t *timeSeriesRangeIterator) NextSeries() (*prompb.TimeSeries, string, error) {
	next, err := t.q.Recv()
	return next.GetTimeseries(), next.GetWarnings(), err
}

func newRangeQuery(logger log.Logger, analyzer *queryAnalyzer, queriers []querypb.QueryClient, qs string, from, to time.Time, interval time.Duration) (*rangeQuery, error) {
	analysis, err := analyzer.Analyze(qs)
	if err != nil {
		return nil, err
	}

	return &rangeQuery{
		logger:   logger,
		queriers: queriers,
		analysis: analysis,
		qs:       qs,
		from:     from,
		to:       to,
		interval: interval,
	}, nil
}

func (q *rangeQuery) Exec(ctx context.Context) *promql.Result {
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

	matrix := make(promql.Matrix, len(series))
	result := promql.Result{
		Value:    matrix,
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

		points := make([]promql.Point, len(s.Samples))
		for i, sample := range s.Samples {
			points[i] = promql.Point{
				T: sample.Timestamp,
				V: sample.Value,
			}
		}

		matrix[i] = promql.Series{
			Metric: labelset,
			Points: points,
		}
	}

	return &result
}

func (q *rangeQuery) getQueryClients(ctx context.Context) ([]timeSeriesIterator, error) {
	numQueriers := len(q.queriers)

	if !q.analysis.IsShardable() {
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

func (q *rangeQuery) queryClientForShard(ctx context.Context, shard *storepb.ShardInfo, numClients int) (timeSeriesIterator, error) {
	var querierIndex int64
	if shard != nil {
		querierIndex = shard.ShardIndex
	} else {
		querierIndex = rand.Int63n(int64(numClients))
	}

	queryClient, err := q.queriers[querierIndex].QueryRange(ctx, &querypb.QueryRangeRequest{
		Query:            q.qs,
		StartTimeSeconds: q.from.Unix(),
		EndTimeSeconds:   q.to.Unix(),
		IntervalSeconds:  int64(q.interval.Seconds()),
		TimeoutSeconds:   int64(60 * time.Second),
		EnableDedup:      true,
		ShardInfo:        shard,
	})
	if err != nil {
		return nil, err
	}

	return &timeSeriesRangeIterator{
		q: queryClient,
	}, nil
}

func (q *rangeQuery) Close() {
	//TODO implement me
	panic("implement me")
}

func (q *rangeQuery) Statement() parser.Statement {
	//TODO implement me
	panic("implement me")
}

func (q *rangeQuery) Stats() *stats.QueryTimers {
	//TODO implement me
	panic("implement me")
}

func (q *rangeQuery) Cancel() {
	if q.cancel != nil {
		q.cancel()
	}
}

func (q *rangeQuery) String() string {
	return q.qs
}
