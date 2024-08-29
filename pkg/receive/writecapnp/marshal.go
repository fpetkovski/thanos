// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"capnproto.org/go/capnp/v3"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func Marshal(tenant string, tsreq []prompb.TimeSeries) ([]byte, error) {
	wr, err := Build(tenant, tsreq)
	if err != nil {
		return nil, err
	}

	return wr.Message().Marshal()
}

func MarshalPacked(tenant string, tsreq []prompb.TimeSeries) ([]byte, error) {
	wr, err := Build(tenant, tsreq)
	if err != nil {
		return nil, err
	}

	return wr.Message().MarshalPacked()
}

func Build(tenant string, tsreq []prompb.TimeSeries) (WriteRequest, error) {
	arena := capnp.SingleSegment(nil)
	_, seg, err := capnp.NewMessage(arena)
	if err != nil {
		return WriteRequest{}, err
	}
	wr, err := NewRootWriteRequest(seg)
	if err != nil {
		return WriteRequest{}, err
	}
	if err := BuildInto(wr, tenant, tsreq); err != nil {
		return WriteRequest{}, err
	}
	return wr, nil
}

func BuildInto(wr WriteRequest, tenant string, tsreq []prompb.TimeSeries) error {
	symbols := make(map[string]uint32)
	for _, ts := range tsreq {
		addLabelsToTable(symbols, ts.Labels)
		for _, e := range ts.Exemplars {
			addLabelsToTable(symbols, e.Labels)
		}
	}

	if err := wr.SetTenant(tenant); err != nil {
		return err
	}

	s, err := wr.NewSymbols()
	if err != nil {
		return err
	}
	items, err := s.NewItems(int32(len(symbols)))
	if err != nil {
		return err
	}
	for k, i := range symbols {
		if err := items.Set(int(i), k); err != nil {
			return err
		}
	}

	series, err := wr.NewTimeSeries(int32(len(tsreq)))
	if err != nil {
		return err
	}
	for i, ts := range tsreq {
		tsc := series.At(i)

		lblsc, err := tsc.NewLabels(int32(len(ts.Labels)))
		if err != nil {
			return err
		}
		if err := marshalLabels(lblsc, ts.Labels, symbols); err != nil {
			return err
		}
		if err := marshalSamples(tsc, ts.Samples); err != nil {
			return err
		}
		if err := marshalHistograms(tsc, ts.Histograms); err != nil {
			return err
		}
		if err := marshalExemplars(tsc, ts.Exemplars, symbols); err != nil {
			return err
		}
	}

	return nil
}

func addLabelsToTable(symbols map[string]uint32, lbls []labelpb.ZLabel) {
	for _, lbl := range lbls {
		if _, ok := symbols[lbl.Name]; !ok {
			symbols[lbl.Name] = uint32(len(symbols))
		}
		if _, ok := symbols[lbl.Value]; !ok {
			symbols[lbl.Value] = uint32(len(symbols))
		}
	}
}

func marshalLabels(lbls Label_List, pbLbls []labelpb.ZLabel, symbols map[string]uint32) error {
	for j, lbl := range pbLbls {
		lbls.At(j).SetName(symbols[lbl.Name])
		lbls.At(j).SetValue(symbols[lbl.Value])
	}
	return nil
}

func marshalSamples(ts TimeSeries, pbSamples []prompb.Sample) error {
	samples, err := ts.NewSamples(int32(len(pbSamples)))
	if err != nil {
		return err
	}

	for j, sample := range pbSamples {
		sc := samples.At(j)
		sc.SetTimestamp(sample.Timestamp)
		sc.SetValue(sample.Value)
	}
	return nil
}

func marshalHistograms(ts TimeSeries, pbHistograms []prompb.Histogram) error {
	if len(pbHistograms) == 0 {
		return nil
	}
	histograms, err := ts.NewHistograms(int32(len(pbHistograms)))
	if err != nil {
		return err
	}
	for i, h := range pbHistograms {
		if err := marshalHistogram(histograms.At(i), h); err != nil {
			return err
		}
	}
	return nil
}

func marshalHistogram(histogram Histogram, h prompb.Histogram) error {
	histogram.SetResetHint(Histogram_ResetHint(h.ResetHint))
	switch h.Count.(type) {
	case *prompb.Histogram_CountInt:
		histogram.Count().SetCountInt(h.GetCountInt())
	case *prompb.Histogram_CountFloat:
		histogram.Count().SetCountFloat(h.GetCountFloat())
	}
	histogram.SetSum(h.Sum)
	histogram.SetSchema(h.Schema)
	histogram.SetZeroThreshold(h.ZeroThreshold)

	switch h.ZeroCount.(type) {
	case *prompb.Histogram_ZeroCountInt:
		histogram.ZeroCount().SetZeroCountInt(h.GetZeroCountInt())
	case *prompb.Histogram_ZeroCountFloat:
		histogram.ZeroCount().SetZeroCountFloat(h.GetZeroCountFloat())
	}

	// Negative spans, deltas and counts.
	negativeSpans, err := histogram.NewNegativeSpans(int32(len(h.NegativeSpans)))
	if err != nil {
		return err
	}
	if err := marshalSpans(negativeSpans, h.NegativeSpans); err != nil {
		return err
	}
	negativeDeltas, err := histogram.NewNegativeDeltas(int32(len(h.NegativeDeltas)))
	if err != nil {
		return err
	}
	marshalInt64List(negativeDeltas, h.NegativeDeltas)

	negativeCounts, err := histogram.NewNegativeCounts(int32(len(h.NegativeCounts)))
	if err != nil {
		return err
	}
	marshalFloat64List(negativeCounts, h.NegativeCounts)

	// Positive spans, deltas and counts.
	positiveSpans, err := histogram.NewPositiveSpans(int32(len(h.PositiveSpans)))
	if err != nil {
		return err
	}
	if err := marshalSpans(positiveSpans, h.PositiveSpans); err != nil {
		return err
	}
	positiveDeltas, err := histogram.NewPositiveDeltas(int32(len(h.PositiveDeltas)))
	if err != nil {
		return err
	}
	marshalInt64List(positiveDeltas, h.PositiveDeltas)

	positiveCounts, err := histogram.NewPositiveCounts(int32(len(h.PositiveCounts)))
	if err != nil {
		return err
	}
	marshalFloat64List(positiveCounts, h.PositiveCounts)

	histogram.SetTimestamp(h.Timestamp)

	return nil
}

func marshalSpans(spans BucketSpan_List, pbSpans []prompb.BucketSpan) error {
	for j, s := range pbSpans {
		span := spans.At(j)
		span.SetOffset(s.Offset)
		span.SetLength(s.Length)
	}
	return nil
}

func marshalExemplars(ts TimeSeries, pbExemplars []prompb.Exemplar, symbols map[string]uint32) error {
	if len(pbExemplars) == 0 {
		return nil
	}

	exemplars, err := ts.NewExemplars(int32(len(pbExemplars)))
	if err != nil {
		return err
	}
	for i := 0; i < len(pbExemplars); i++ {
		ex := exemplars.At(i)

		lbls, err := ex.NewLabels(int32(len(pbExemplars[i].Labels)))
		if err != nil {
			return err
		}
		if err := marshalLabels(lbls, pbExemplars[i].Labels, symbols); err != nil {
			return err
		}
		ex.SetValue(pbExemplars[i].Value)
		ex.SetTimestamp(pbExemplars[i].Timestamp)
	}
	return nil
}

func marshalInt64List(list capnp.Int64List, ints []int64) {
	for j, d := range ints {
		list.Set(j, d)
	}
}

func marshalFloat64List(list capnp.Float64List, ints []float64) {
	for j, d := range ints {
		list.Set(j, d)
	}
}
