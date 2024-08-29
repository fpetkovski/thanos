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

	return BuildWithSegment(tenant, tsreq, seg)
}

func BuildWithSegment(tenant string, tsreq []prompb.TimeSeries, seg *capnp.Segment) (WriteRequest, error) {
	symbols := make(map[string]uint32)
	for _, ts := range tsreq {
		addLabelsToTable(symbols, ts.Labels)
		for _, e := range ts.Exemplars {
			addLabelsToTable(symbols, e.Labels)
		}
	}

	wr, err := NewRootWriteRequest(seg)
	if err != nil {
		return WriteRequest{}, err
	}
	if err := wr.SetTenant(tenant); err != nil {
		return WriteRequest{}, err
	}

	s, err := NewSymbols(seg)
	if err != nil {
		return WriteRequest{}, err
	}
	items, err := s.NewItems(int32(len(symbols)))
	if err != nil {
		return WriteRequest{}, err
	}
	for k, i := range symbols {
		if err := items.Set(int(i), k); err != nil {
			return WriteRequest{}, err
		}
	}
	if err := wr.SetSymbols(s); err != nil {
		return WriteRequest{}, err
	}

	series, err := NewTimeSeries_List(seg, int32(len(tsreq)))
	if err != nil {
		return WriteRequest{}, err
	}
	for i, ts := range tsreq {
		tsc, err := NewTimeSeries(seg)
		if err != nil {
			return WriteRequest{}, err
		}
		lblsc, err := marshalLabels(seg, ts.Labels, symbols)
		if err != nil {
			return WriteRequest{}, err
		}
		if err := tsc.SetLabels(lblsc); err != nil {
			return WriteRequest{}, err
		}

		samples, err := marshalSamples(seg, ts.Samples)
		if err != nil {
			return WriteRequest{}, err
		}
		if err := tsc.SetSamples(samples); err != nil {
			return WriteRequest{}, err
		}

		histograms, err := marshalHistograms(seg, ts.Histograms)
		if err != nil {
			return WriteRequest{}, err
		}
		if err := tsc.SetHistograms(histograms); err != nil {
			return WriteRequest{}, err
		}

		exemplars, err := marshalExemplars(seg, ts.Exemplars, symbols)
		if err != nil {
			return WriteRequest{}, err
		}
		if err := tsc.SetExemplars(exemplars); err != nil {
			return WriteRequest{}, err
		}

		if err := series.Set(i, tsc); err != nil {
			return WriteRequest{}, err
		}
	}
	if err := wr.SetTimeSeries(series); err != nil {
		return WriteRequest{}, err
	}

	return wr, nil
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

func marshalLabels(seg *capnp.Segment, lbls []labelpb.ZLabel, symbols map[string]uint32) (Label_List, error) {
	lblsc, err := NewLabel_List(seg, int32(len(lbls)))
	if err != nil {
		return Label_List{}, err
	}
	for j, lbl := range lbls {
		lblc, err := NewLabel(seg)
		lblc.SetName(symbols[lbl.Name])
		lblc.SetValue(symbols[lbl.Value])
		if err != nil {
			return Label_List{}, err
		}
		if err := lblsc.Set(j, lblc); err != nil {
			return Label_List{}, err
		}
	}

	return lblsc, nil
}

func marshalSamples(seg *capnp.Segment, pbSamples []prompb.Sample) (Sample_List, error) {
	samples, err := NewSample_List(seg, int32(len(pbSamples)))
	if err != nil {
		return Sample_List{}, err
	}

	for j, sample := range pbSamples {
		sc, err := NewSample(seg)
		if err != nil {
			return Sample_List{}, err
		}
		sc.SetTimestamp(sample.Timestamp)
		sc.SetValue(sample.Value)
		if err := samples.Set(j, sc); err != nil {
			return Sample_List{}, err
		}
	}
	return samples, nil
}

func marshalHistograms(seg *capnp.Segment, pbHistograms []prompb.Histogram) (Histogram_List, error) {
	histograms, err := NewHistogram_List(seg, int32(len(pbHistograms)))
	if err != nil {
		return Histogram_List{}, err
	}
	for i, h := range pbHistograms {
		histogram, err := marshalHistogram(seg, h)
		if err != nil {
			return Histogram_List{}, err
		}
		if err := histograms.Set(i, histogram); err != nil {
			return Histogram_List{}, err
		}
	}
	return histograms, nil
}

func marshalHistogram(seg *capnp.Segment, h prompb.Histogram) (Histogram, error) {
	histogram, err := NewHistogram(seg)
	if err != nil {
		return Histogram{}, err
	}

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
	negativeSpans, err := marshalSpans(seg, h.NegativeSpans)
	if err != nil {
		return Histogram{}, err
	}
	if err := histogram.SetNegativeSpans(negativeSpans); err != nil {
		return Histogram{}, err
	}
	negativeDeltas, err := marshalInt64List(seg, h.NegativeDeltas)
	if err != nil {
		return Histogram{}, err
	}
	if err := histogram.SetNegativeDeltas(negativeDeltas); err != nil {
		return Histogram{}, err
	}
	negativeCounts, err := marshalFloat64List(seg, h.NegativeCounts)
	if err != nil {
		return Histogram{}, err
	}
	if err := histogram.SetNegativeCounts(negativeCounts); err != nil {
		return Histogram{}, err
	}

	// Positive spans, deltas and counts.
	positiveSpans, err := marshalSpans(seg, h.PositiveSpans)
	if err != nil {
		return Histogram{}, err
	}
	if err := histogram.SetPositiveSpans(positiveSpans); err != nil {
		return Histogram{}, err
	}
	positiveDeltas, err := marshalInt64List(seg, h.PositiveDeltas)
	if err != nil {
		return Histogram{}, err
	}
	if err := histogram.SetPositiveDeltas(positiveDeltas); err != nil {
		return Histogram{}, err
	}
	positiveCounts, err := marshalFloat64List(seg, h.PositiveCounts)
	if err != nil {
		return Histogram{}, err
	}
	if err := histogram.SetPositiveCounts(positiveCounts); err != nil {
		return Histogram{}, err
	}
	histogram.SetTimestamp(h.Timestamp)

	return histogram, nil
}

func marshalSpans(seg *capnp.Segment, pbSpans []prompb.BucketSpan) (BucketSpan_List, error) {
	spans, err := NewBucketSpan_List(seg, int32(len(pbSpans)))
	if err != nil {
		return BucketSpan_List{}, err
	}
	for j, s := range pbSpans {
		span, err := NewBucketSpan(seg)
		if err != nil {
			return BucketSpan_List{}, err
		}
		span.SetOffset(s.Offset)
		span.SetLength(s.Length)
		if err := spans.Set(j, span); err != nil {
			return BucketSpan_List{}, err
		}
	}
	return spans, nil
}

func marshalExemplars(seg *capnp.Segment, pbExemplars []prompb.Exemplar, symbols map[string]uint32) (Exemplar_List, error) {
	exemplars, err := NewExemplar_List(seg, int32(len(pbExemplars)))
	if err != nil {
		return Exemplar_List{}, err
	}
	for i := 0; i < len(pbExemplars); i++ {
		ex, err := NewExemplar(seg)
		if err != nil {
			return Exemplar_List{}, err
		}
		lbs, err := marshalLabels(seg, pbExemplars[i].Labels, symbols)
		if err != nil {
			return Exemplar_List{}, err
		}
		if err := ex.SetLabels(lbs); err != nil {
			return Exemplar_List{}, err
		}
		ex.SetValue(pbExemplars[i].Value)
		ex.SetTimestamp(pbExemplars[i].Timestamp)
		if err := exemplars.Set(i, ex); err != nil {
			return Exemplar_List{}, err
		}
	}
	return exemplars, nil
}

func marshalInt64List(seg *capnp.Segment, ints []int64) (capnp.Int64List, error) {
	list, err := capnp.NewInt64List(seg, int32(len(ints)))
	if err != nil {
		return capnp.Int64List{}, err
	}
	for j, d := range ints {
		list.Set(j, d)
	}
	return list, nil
}

func marshalFloat64List(seg *capnp.Segment, ints []float64) (capnp.Float64List, error) {
	list, err := capnp.NewFloat64List(seg, int32(len(ints)))
	if err != nil {
		return capnp.Float64List{}, err
	}
	for j, d := range ints {
		list.Set(j, d)
	}
	return list, nil
}
