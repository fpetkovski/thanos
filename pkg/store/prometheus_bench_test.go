// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func BenchmarkPrometheusStore_Series_WithSharding(b *testing.B) {
	p, err := e2eutil.NewPrometheus()
	testutil.Ok(b, err)
	defer func() { testutil.Ok(b, p.Stop()) }()

	var timeSeries []labels.Labels
	for i := 0; i < 1000; i++ {
		timeSeries = append(timeSeries, []labels.Label{
			{Name: labels.MetricName, Value: "http_requests_total"},
			{Name: "pod", Value: strconv.Itoa(i)},
			{Name: "handler", Value: "/"},
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := model.Now()
	from := timestamp.FromTime(now.Time().Add(-1 * time.Hour))
	to := timestamp.FromTime(now.Time().Add(1 * time.Hour))
	func() {
		b.Helper()
		_, err = e2eutil.CreateBlock(ctx, p.Dir(), timeSeries, 1000, from, to, nil, 0, metadata.NoneFunc)
	}()
	testutil.Ok(b, p.Start())

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(b, err)

	promStore, err := NewPrometheusStore(nil, nil, promclient.NewDefaultClient(), u, component.Sidecar,
		func() labels.Labels { return labels.FromStrings("region", "eu-west") },
		func() (int64, int64) { return math.MinInt64/1000 + 62135596801, math.MaxInt64/1000 - 62135596801 }, nil)
	testutil.Ok(b, err)

	srv := newStoreSeriesServer(ctx)
	request := &storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "http_requests_total"},
		},
		MinTime: from,
		MaxTime: to,
		ShardInfo: &storepb.ShardInfo{
			ShardIndex:  0,
			TotalShards: 2,
			By:          true,
			Labels:      []string{"pod"},
		},
	}

	cpuProfileFile, err := os.Create("./cpu.pprof")
	testutil.Ok(b, err)

	testutil.Ok(b, pprof.StartCPUProfile(cpuProfileFile))
	defer pprof.StopCPUProfile()

	traceFile, err := os.Create("./trace.out")
	testutil.Ok(b, err)
	testutil.Ok(b, trace.Start(traceFile))
	defer trace.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = promStore.Series(request, srv)
		testutil.Ok(b, err)
	}
}
