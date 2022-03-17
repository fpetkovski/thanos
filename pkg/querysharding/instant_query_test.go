package querysharding_test

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestQueryExec(t *testing.T) {
	now := time.Now()

	lbls1 := labels.FromStrings("__name__", "http_requests_total", "pod", "nginx-1")
	sample1 := prompb.Sample{Timestamp: now.Unix(), Value: 10}
	series1 := newSeries(lbls1, sample1)

	lbls2 := labels.FromStrings("__name__", "http_requests_total", "pod", "nginx-2")
	sample2 := prompb.Sample{Timestamp: now.Unix(), Value: 15}
	series2 := newSeries(lbls2, sample2)

	logger := log.NewNopLogger()
	queriers := []querypb.QueryClient{
		newQueryClient(series1),
		newQueryClient(series2),
	}
	engine := querysharding.NewEngine(logger, func() []querypb.QueryClient {
		return queriers
	})
	qry, err := engine.NewInstantQuery(nil, "sum by (pod) (http_requests_total)", now)
	testutil.Ok(t, err)

	result := qry.Exec(context.Background())
	expected := &promql.Result{
		Value: promql.Vector{
			{
				Metric: lbls1,
				Point: promql.Point{
					T: now.Unix(),
					V: 10,
				},
			},
			{
				Metric: lbls2,
				Point: promql.Point{
					T: now.Unix(),
					V: 15,
				},
			},
		},
	}
	vector, _ := result.Vector()
	sort.Slice(vector, func(i, j int) bool {
		return vector[i].Metric.String() < vector[j].Metric.String()
	})
	testutil.Equals(t, expected, result)
}

func TestQueryExecCancel(t *testing.T) {
	now := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	logger := log.NewLogfmtLogger(os.Stdout)
	queriers := []querypb.QueryClient{newCancellableClient(ctx)}
	engine := querysharding.NewEngine(logger, func() []querypb.QueryClient {
		return queriers
	})
	qry, err := engine.NewInstantQuery(nil, "sum by (pod) (http_requests_total)", now)
	testutil.Ok(t, err)

	go func() {
		<-time.After(time.Second)
		cancel()
	}()

	result := qry.Exec(ctx)
	testutil.Equals(t, promql.ErrQueryCanceled("query cancelled"), result.Err)
}

func TestQueryExecWithError(t *testing.T) {
	now := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	logger := log.NewLogfmtLogger(os.Stdout)
	queriers := []querypb.QueryClient{newCancellableClient(ctx)}
	engine := querysharding.NewEngine(logger, func() []querypb.QueryClient {
		return queriers
	})
	qry, err := engine.NewInstantQuery(nil, "sum by (pod) (http_requests_total)", now)
	testutil.Ok(t, err)

	go func() {
		<-time.After(time.Second)
		cancel()
	}()

	result := qry.Exec(ctx)
	testutil.Equals(t, promql.ErrQueryCanceled("query cancelled"), result.Err)
}

func newSeries(lbls labels.Labels, sample prompb.Sample) []*prompb.TimeSeries {
	return []*prompb.TimeSeries{
		{
			Labels:  labelpb.ZLabelsFromPromLabels(lbls),
			Samples: []prompb.Sample{sample},
		},
	}
}
