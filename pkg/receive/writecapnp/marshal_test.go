// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"fmt"
	"testing"

	"capnproto.org/go/capnp/v3"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestMarshalWriteRequest(t *testing.T) {
	wreq := storepb.WriteRequest{
		Tenant: "example-tenant",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "up"},
					{Name: "job", Value: "prometheus"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 1},
					{Timestamp: 2, Value: 2},
				},
				Histograms: []prompb.Histogram{
					prompb.HistogramToHistogramProto(1, tsdbutil.GenerateTestHistogram(1)),
					prompb.FloatHistogramToHistogramProto(2, tsdbutil.GenerateTestFloatHistogram(2)),
				},
				Exemplars: []prompb.Exemplar{
					{
						Labels:    []labelpb.ZLabel{{Name: "traceID", Value: "1234"}},
						Value:     10,
						Timestamp: 14,
					},
				},
			},
			{
				Labels: []labelpb.ZLabel{
					{Name: "__name__", Value: "up"},
					{Name: "job", Value: "thanos"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 3, Value: 3},
					{Timestamp: 4, Value: 4},
				},
			},
		},
	}
	b, err := Marshal(wreq.Tenant, wreq.Timeseries)
	require.NoError(t, err)

	msg, err := capnp.Unmarshal(b)
	require.NoError(t, err)

	wr, err := ReadRootWriteRequest(msg)
	require.NoError(t, err)

	tenant, err := wr.Tenant()
	require.NoError(t, err)
	require.Equal(t, wreq.Tenant, tenant)

	series, err := wr.TimeSeries()
	require.NoError(t, err)
	require.Equal(t, len(wreq.Timeseries), series.Len())
	var (
		i       int
		request = NewWriteableRequest(wr)
	)
	var actual prompb.TimeSeries
	for request.Next() {
		request.At(&actual)
		expected := wreq.Timeseries[i]
		if expected.Exemplars == nil {
			expected.Exemplars = make([]prompb.Exemplar, 0)
		}
		if expected.Histograms == nil {
			expected.Histograms = make([]prompb.Histogram, 0)
		}
		require.Equal(t, expected, actual, fmt.Sprintf("incorrect series at %d", i))
		i++
	}
}

func TestMarshalWithMultipleHistogramSeries(t *testing.T) {
	wreq := storepb.WriteRequest{
		Tenant: "example-tenant",
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []labelpb.ZLabel{
					{Name: "job", Value: "prometheus-1"},
				},
				Histograms: []prompb.Histogram{
					prompb.HistogramToHistogramProto(1, &histogram.Histogram{}),
					prompb.HistogramToHistogramProto(1, tsdbutil.GenerateTestHistogram(1)),
					prompb.FloatHistogramToHistogramProto(2, tsdbutil.GenerateTestFloatHistogram(2)),
				},
			},
			{
				Labels: []labelpb.ZLabel{
					{Name: "job", Value: "prometheus-2"},
				},
				Histograms: []prompb.Histogram{
					prompb.HistogramToHistogramProto(1, tsdbutil.GenerateTestHistogram(1)),
					prompb.FloatHistogramToHistogramProto(2, tsdbutil.GenerateTestFloatHistogram(2)),
					prompb.HistogramToHistogramProto(1, &histogram.Histogram{}),
				},
			},
		},
	}
	b, err := Marshal(wreq.Tenant, wreq.Timeseries)
	require.NoError(t, err)

	msg, err := capnp.Unmarshal(b)
	require.NoError(t, err)

	wr, err := ReadRootWriteRequest(msg)
	require.NoError(t, err)

	tenant, err := wr.Tenant()
	require.NoError(t, err)
	require.Equal(t, wreq.Tenant, tenant)

	series, err := wr.TimeSeries()
	require.NoError(t, err)
	require.Equal(t, len(wreq.Timeseries), series.Len())
	var (
		request = NewWriteableRequest(wr)
		current prompb.TimeSeries

		readHistograms      []*histogram.Histogram
		readFloatHistograms []*histogram.FloatHistogram
	)
	for request.Next() {
		request.At(&current)
		current.Labels = labelpb.ZLabelsFromPromLabels(labelpb.ZLabelsToPromLabels(current.Labels).Copy())
		for _, h := range current.Histograms {
			if h.IsFloatHistogram() {
				readFloatHistograms = append(readFloatHistograms, prompb.FloatHistogramProtoToFloatHistogram(h))
			} else {
				readHistograms = append(readHistograms, prompb.HistogramProtoToHistogram(h))
			}
		}
	}
	var (
		histograms      []*histogram.Histogram
		floatHistograms []*histogram.FloatHistogram
	)
	for _, ts := range wreq.Timeseries {
		for _, h := range ts.Histograms {
			if h.IsFloatHistogram() {
				floatHistograms = append(floatHistograms, prompb.FloatHistogramProtoToFloatHistogram(h))
			} else {
				histograms = append(histograms, prompb.HistogramProtoToHistogram(h))
			}
		}
	}
	require.Equal(t, histograms, readHistograms)
	require.Equal(t, floatHistograms, readFloatHistograms)
}
