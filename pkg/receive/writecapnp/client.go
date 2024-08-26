// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"context"
	"net"
	"sync"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

type RemoteWriteClient struct {
	mu sync.Mutex

	addr   string
	conn   *net.TCPConn
	writer Writer
}

func NewRemoteWriteClient(addr string) *RemoteWriteClient {
	return &RemoteWriteClient{addr: addr}
}

func (r *RemoteWriteClient) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, _ ...grpc.CallOption) (*storepb.WriteResponse, error) {
	return r.writeWithReconnect(ctx, 2, in)
}

func (r *RemoteWriteClient) writeWithReconnect(ctx context.Context, numReconnects int, in *storepb.WriteRequest) (*storepb.WriteResponse, error) {
	if err := r.connect(ctx); err != nil {
		return nil, err
	}
	result, release := r.writer.Write(ctx, func(params Writer_write_Params) error {
		wr, err := Build(in.Tenant, in.Timeseries)
		if err != nil {
			return err
		}
		return params.SetWr(wr)
	})
	defer release()

	s, err := result.Struct()
	if err != nil {
		if numReconnects > 0 && capnp.IsDisconnected(err) {
			r.disconnect()
			numReconnects--
			return r.writeWithReconnect(ctx, numReconnects, in)
		}
		return nil, errors.Wrap(err, "failed writing to peer")
	}
	switch s.Error() {
	case WriteError_unavailable:
		return nil, status.Error(codes.Unavailable, "rpc failed")
	case WriteError_alreadyExists:
		return nil, status.Error(codes.AlreadyExists, "rpc failed")
	case WriteError_invalidArgument:
		return nil, status.Error(codes.InvalidArgument, "rpc failed")
	case WriteError_internal:
		return nil, status.Error(codes.Internal, "rpc failed")
	default:
		return &storepb.WriteResponse{}, nil
	}
}

func (r *RemoteWriteClient) connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		return nil
	}

	addr, err := net.ResolveTCPAddr("tcp", r.addr)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return errors.Wrap(err, "failed to dial peer")
	}

	r.writer = Writer(rpc.NewConn(rpc.NewPackedStreamTransport(conn), nil).Bootstrap(ctx))
	r.conn = conn
	return nil
}

func (r *RemoteWriteClient) disconnect() {
	r.mu.Lock()
	if r.conn != nil {
		conn := r.conn
		r.conn = nil
		go conn.Close()
	}
	r.mu.Unlock()
}

type WriteableRequest struct {
	i       int
	symbols []string
	series  TimeSeries_List
}

func NewWriteableRequest(wr WriteRequest) *WriteableRequest {
	ts, _ := wr.TimeSeries()
	symTable, _ := wr.Symbols()
	symbols, _ := symTable.Items()

	strings := make([]string, 0, symbols.Len())
	for i := 0; i < symbols.Len(); i++ {
		sym, _ := symbols.At(i)
		strings = append(strings, sym)
	}

	return &WriteableRequest{
		i:       -1,
		symbols: strings,
		series:  ts,
	}
}

func (s *WriteableRequest) Next() bool {
	s.i++
	return s.i < s.series.Len()
}

func (s *WriteableRequest) At(t *prompb.TimeSeries) {
	lbls, err := s.series.At(s.i).Labels()
	if err != nil {
		panic(err)
	}
	t.Labels = resizeSlice(t.Labels, lbls.Len())
	for i := 0; i < lbls.Len(); i++ {
		lbl := lbls.At(i)
		t.Labels[i].Name = s.symbols[lbl.Name()]
		t.Labels[i].Value = s.symbols[lbl.Value()]
	}

	samples, err := s.series.At(s.i).Samples()
	if err != nil {
		panic(err)
	}
	t.Samples = resizeSlice(t.Samples, samples.Len())
	for i := 0; i < samples.Len(); i++ {
		sample := samples.At(i)
		t.Samples[i].Timestamp = sample.Timestamp()
		t.Samples[i].Value = sample.Value()
	}

	histograms, err := s.series.At(s.i).Histograms()
	if err != nil {
		panic(err)
	}
	t.Histograms = resizeSlice(t.Histograms, histograms.Len())
	for i := 0; i < histograms.Len(); i++ {
		s.readHistogram(&t.Histograms[i], histograms.At(i))
	}

	exemplars, err := s.series.At(s.i).Exemplars()
	if err != nil {
		panic(err)
	}
	t.Exemplars = resizeSlice(t.Exemplars, exemplars.Len())
	for i := 0; i < exemplars.Len(); i++ {
		e := exemplars.At(i)
		if err := s.readExemplar(s.symbols, &t.Exemplars[i], e); err != nil {
			panic(err)
		}
	}

}

func (s *WriteableRequest) readHistogram(pbHistogram *prompb.Histogram, h Histogram) {
	pbHistogram.ResetHint = prompb.Histogram_ResetHint(h.ResetHint())

	switch h.Count().Which() {
	case Histogram_count_Which_countInt:
		pbHistogram.Count = &prompb.Histogram_CountInt{CountInt: h.Count().CountInt()}
	case Histogram_count_Which_countFloat:
		pbHistogram.Count = &prompb.Histogram_CountFloat{CountFloat: h.Count().CountFloat()}
	}

	pbHistogram.Sum = h.Sum()
	pbHistogram.Schema = h.Schema()
	pbHistogram.ZeroThreshold = h.ZeroThreshold()

	switch h.ZeroCount().Which() {
	case Histogram_zeroCount_Which_zeroCountInt:
		pbHistogram.ZeroCount = &prompb.Histogram_ZeroCountInt{ZeroCountInt: h.ZeroCount().ZeroCountInt()}
	case Histogram_zeroCount_Which_zeroCountFloat:
		pbHistogram.ZeroCount = &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: h.ZeroCount().ZeroCountFloat()}
	}

	// Negative spans, counts and deltas.
	negativeSpans, err := h.NegativeSpans()
	if err != nil {
		panic(err)
	}
	if negativeSpans.Len() > 0 {
		pbHistogram.NegativeSpans = make([]prompb.BucketSpan, negativeSpans.Len())
		for j := 0; j < len(pbHistogram.NegativeSpans); j++ {
			pbHistogram.NegativeSpans[j].Offset = negativeSpans.At(j).Offset()
			pbHistogram.NegativeSpans[j].Length = negativeSpans.At(j).Length()
		}
	}
	negativeDeltas, err := h.NegativeDeltas()
	if err != nil {
		panic(err)
	}
	if negativeDeltas.Len() > 0 {
		pbHistogram.NegativeDeltas = make([]int64, negativeDeltas.Len())
		for j := 0; j < negativeDeltas.Len(); j++ {
			pbHistogram.NegativeDeltas[j] = negativeDeltas.At(j)
		}
	}
	negativeCounts, err := h.NegativeCounts()
	if err != nil {
		panic(err)
	}
	if negativeCounts.Len() > 0 {
		pbHistogram.NegativeCounts = make([]float64, negativeCounts.Len())
		for j := 0; j < negativeCounts.Len(); j++ {
			pbHistogram.NegativeCounts[j] = negativeCounts.At(j)
		}
	}

	// Positive spans, counts and deltas.
	positiveSpans, err := h.PositiveSpans()
	if err != nil {
		panic(err)
	}
	if positiveSpans.Len() > 0 {
		pbHistogram.PositiveSpans = make([]prompb.BucketSpan, positiveSpans.Len())
		for j := 0; j < len(pbHistogram.PositiveSpans); j++ {
			pbHistogram.PositiveSpans[j].Offset = positiveSpans.At(j).Offset()
			pbHistogram.PositiveSpans[j].Length = positiveSpans.At(j).Length()
		}
	}
	positiveDeltas, err := h.PositiveDeltas()
	if err != nil {
		panic(err)
	}
	if positiveDeltas.Len() > 0 {
		pbHistogram.PositiveDeltas = make([]int64, positiveDeltas.Len())
		for j := 0; j < positiveDeltas.Len(); j++ {
			pbHistogram.PositiveDeltas[j] = positiveDeltas.At(j)
		}
	}
	positiveCounts, err := h.PositiveCounts()
	if err != nil {
		panic(err)
	}
	if positiveCounts.Len() > 0 {
		pbHistogram.PositiveCounts = make([]float64, positiveCounts.Len())
		for j := 0; j < positiveCounts.Len(); j++ {
			pbHistogram.PositiveCounts[j] = positiveCounts.At(j)
		}
	}

	pbHistogram.Timestamp = h.Timestamp()
}

func (s *WriteableRequest) readExemplar(symbols []string, exemplar *prompb.Exemplar, e Exemplar) error {
	lbls, err := e.Labels()
	if err != nil {
		return err
	}
	exemplar.Labels = make([]labelpb.ZLabel, lbls.Len())
	for i := 0; i < lbls.Len(); i++ {
		exemplar.Labels[i].Name = symbols[lbls.At(i).Name()]
		exemplar.Labels[i].Value = symbols[lbls.At(i).Value()]
	}
	exemplar.Value = e.Value()
	exemplar.Timestamp = e.Timestamp()

	return nil
}

func resizeSlice[T any](slice []T, sz int) []T {
	if slice == nil && sz == 0 {
		return nil
	}
	if cap(slice) >= sz {
		return slice[:sz]
	}
	return make([]T, sz)
}
