package querysharding_test

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQueryRangeExec(t *testing.T) {
	now := time.Now()

	lbls1 := labels.FromStrings("__name__", "http_requests_total", "pod", "nginx-1")
	sample1 := prompb.Sample{Timestamp: now.Unix(), Value: 10}
	series1 := newSeries(lbls1, sample1)

	lbls2 := labels.FromStrings("__name__", "http_requests_total", "pod", "nginx-2")
	sample2 := prompb.Sample{Timestamp: now.Unix(), Value: 15}
	series2 := newSeries(lbls2, sample2)

	logger := log.NewNopLogger()
	queriers := []querypb.QueryClient{
		newQueryRangeClient(series1),
		newQueryRangeClient(series2),
	}
	engine := querysharding.NewEngine(logger, func() []querypb.QueryClient {
		return queriers
	})
	qry, err := engine.NewRangeQuery(nil, "sum by (pod) (http_requests_total)", now, now.Add(2*time.Second), 1)
	testutil.Ok(t, err)

	result := qry.Exec(context.Background())
	expected := &promql.Result{
		Value: promql.Matrix{
			{
				Metric: lbls1,
				Points: []promql.Point{{
					T: now.Unix(),
					V: 10,
				}},
			},
			{
				Metric: lbls2,
				Points: []promql.Point{{
					T: now.Unix(),
					V: 15,
				}},
			},
		},
	}
	matrix, _ := result.Matrix()
	sort.Slice(matrix, func(i, j int) bool {
		return matrix[i].Metric.String() < matrix[j].Metric.String()
	})
	testutil.Equals(t, expected, result)
}

func TestRangeQueryExecCancel(t *testing.T) {
	now := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	logger := log.NewLogfmtLogger(os.Stdout)

	querier := newQueryRangeClientStub(func() (*querypb.QueryRangeResponse, error) {
		<-ctx.Done()
		return nil, status.Error(codes.Canceled, "cancelled")
	})
	engine := querysharding.NewEngine(logger, func() []querypb.QueryClient {
		return []querypb.QueryClient{querier}
	})

	qry, err := engine.NewRangeQuery(nil, "sum by (pod) (http_requests_total)", now, now.Add(5*time.Minute), 30*time.Second)
	testutil.Ok(t, err)

	go func() {
		<-time.After(time.Second)
		cancel()
	}()

	result := qry.Exec(ctx)
	expected := &promql.Result{Err: promql.ErrQueryCanceled("query cancelled")}
	testutil.Equals(t, expected, result)
}

func TestRangeQueryExecError(t *testing.T) {
	now := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	logger := log.NewLogfmtLogger(os.Stdout)

	expectedErr := errors.New("connection lost")
	querier := newQueryRangeClientStub(func() (*querypb.QueryRangeResponse, error) {
		return nil, expectedErr
	})
	engine := querysharding.NewEngine(logger, func() []querypb.QueryClient {
		return []querypb.QueryClient{querier}
	})

	qry, err := engine.NewRangeQuery(nil, "sum by (pod) (http_requests_total)", now, now.Add(5*time.Minute), 30*time.Second)
	testutil.Ok(t, err)

	go func() {
		<-time.After(time.Second)
		cancel()
	}()

	result := qry.Exec(ctx)
	expected := &promql.Result{Err: expectedErr}
	testutil.Equals(t, expected, result)
}
