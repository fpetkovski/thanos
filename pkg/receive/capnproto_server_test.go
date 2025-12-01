// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type tuple[T any, K any] struct {
	A T
	B K
}

func TestCapNProtoServer_SingleSerialClient(t *testing.T) {
	custom.TolerantVerifyLeak(t)
	var (
		logger = log.NewNopLogger()
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener, zstdListener = bufconn.Listen(1024), bufconn.Listen(1024)
		handler                = NewCapNProtoHandler(logger, writer, gate.NewNoop())
	)
	protocols := map[string]tuple[writecapnp.Dialer, writecapnp.NewCodecFunc]{
		"packed": {listener, writecapnp.NewPackedCodec},
		"zstd":   {zstdListener, writecapnp.NewZSTDCodec},
	}
	for name, protocol := range protocols {
		srv := NewCapNProtoServer(listener, zstdListener, handler, logger)
		go func() {
			_ = srv.ListenAndServe()
		}()
		t.Cleanup(srv.Shutdown)

		t.Run(name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				client := writecapnp.NewRemoteWriteClient(protocol.A, protocol.B, logger)
				_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
					Tenant:     "default",
					Timeseries: makeSeriesBatch(),
				})
				require.NoError(t, err)
				require.NoError(t, client.Close())
			}
		})
	}
	require.NoError(t, errors.Join(listener.Close(), zstdListener.Close()))
}

func TestCapNProtoServer_SingleParallelClient(t *testing.T) {
	custom.TolerantVerifyLeak(t)
	var (
		logger = log.NewNopLogger()
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener, zstdListener = bufconn.Listen(1024), bufconn.Listen(1024)
		handler                = NewCapNProtoHandler(logger, writer, gate.NewNoop())
	)
	protocols := map[string]tuple[writecapnp.Dialer, writecapnp.NewCodecFunc]{
		"packed": {listener, writecapnp.NewPackedCodec},
		"zstd":   {zstdListener, writecapnp.NewZSTDCodec},
	}
	for name, protocol := range protocols {
		srv := NewCapNProtoServer(listener, zstdListener, handler, logger)
		go func() {
			_ = srv.ListenAndServe()
		}()
		t.Cleanup(srv.Shutdown)

		t.Run(name, func(t *testing.T) {
			client := writecapnp.NewRemoteWriteClient(protocol.A, protocol.B, logger)
			_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
				Tenant:     "default",
				Timeseries: makeSeriesBatch(),
			})
			for i := 0; i < 100; i++ {
				require.NoError(t, err)
				require.NoError(t, client.Close())
			}
		})
	}
	require.NoError(t, errors.Join(listener.Close(), zstdListener.Close()))
}

func TestCapNProtoServer_MultipleSerialClients(t *testing.T) {
	custom.TolerantVerifyLeak(t)
	var (
		logger = log.NewNopLogger()
		writer = NewCapNProtoWriter(
			logger,
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener, zstdListener = bufconn.Listen(1024), bufconn.Listen(1024)
		handler                = NewCapNProtoHandler(logger, writer, gate.NewNoop())
	)

	protocols := map[string]tuple[writecapnp.Dialer, writecapnp.NewCodecFunc]{
		"packed": {listener, writecapnp.NewPackedCodec},
		"zstd":   {zstdListener, writecapnp.NewZSTDCodec},
	}
	for name, protocol := range protocols {
		srv := NewCapNProtoServer(listener, zstdListener, handler, logger)
		go func() {
			_ = srv.ListenAndServe()
		}()
		t.Cleanup(srv.Shutdown)
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				client := writecapnp.NewRemoteWriteClient(protocol.A, protocol.B, logger)
				_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
					Tenant:     "default",
					Timeseries: makeSeriesBatch(),
				})
				require.NoError(t, err)
				require.NoError(t, client.Close())
			}
		})
	}
	require.NoError(t, errors.Join(listener.Close(), zstdListener.Close()))
}

func TestCapNProtoServer_MultipleSerialClientsWithReconnect(t *testing.T) {
	custom.TolerantVerifyLeak(t)
	var (
		logger              = log.NewNopLogger()
		listener, zstdCodec = bufconn.Listen(1024), bufconn.Listen(1024)
		handler             = newFaultyHandler(2)
	)

	srv := NewCapNProtoServer(listener, zstdCodec, handler, logger)
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(srv.Shutdown)

	client := writecapnp.NewRemoteWriteClient(listener, writecapnp.NewPackedCodec, logger)
	const numRuns = 100
	const numRequests = 10
	for range numRuns {
		handler.numFailures = 2
		var wg sync.WaitGroup
		for range numRequests {
			wg.Go(func() {
				_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
					Tenant:     "default",
					Timeseries: makeSeriesBatch(),
				})
				require.NoError(t, err)
			})
		}
		wg.Wait()
	}
	require.NoError(t, client.Close())
	require.NoError(t, errors.Join(listener.Close(), zstdCodec.Close()))
}

type faultyHandler struct {
	mu          sync.Mutex
	numReqs     int
	numFailures int
}

func newFaultyHandler(failEach int) *faultyHandler {
	return &faultyHandler{numFailures: failEach}
}

func (f *faultyHandler) Write(ctx context.Context, call writecapnp.Writer_write) error {
	call.Go()
	if err := f.checkFailures(); err != nil {
		return err
	}

	arg, err := call.Args().Wr()
	if err != nil {
		return err
	}
	if _, err := arg.Tenant(); err != nil {
		return err
	}
	results, err := call.AllocResults()
	if err != nil {
		return err
	}
	results.SetError(writecapnp.WriteError_none)
	return nil
}

func (f *faultyHandler) checkFailures() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.numFailures--
	if f.numFailures > 0 {
		return errors.New("handler failure")
	}
	return nil
}

func TestCapNProtoServer_MultipleParallelClients(t *testing.T) {
	custom.TolerantVerifyLeak(t)
	var (
		logger = log.NewNopLogger()
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener, zstdListener = bufconn.Listen(1024), bufconn.Listen(1024)
		handler                = NewCapNProtoHandler(logger, writer, gate.NewNoop())
	)

	protocols := map[string]tuple[writecapnp.Dialer, writecapnp.NewCodecFunc]{
		"packed": {listener, writecapnp.NewPackedCodec},
		"zstd":   {zstdListener, writecapnp.NewZSTDCodec},
	}
	for name, protocol := range protocols {
		var (
			client = writecapnp.NewRemoteWriteClient(protocol.A, protocol.B, logger)
			srv    = NewCapNProtoServer(listener, zstdListener, handler, logger)
		)
		go func() {
			_ = srv.ListenAndServe()
		}()
		t.Cleanup(srv.Shutdown)
		t.Run(name, func(t *testing.T) {
			var wg sync.WaitGroup
			for i := 0; i < 1; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
						Tenant:     "default",
						Timeseries: makeSeriesBatch(),
					})
					require.NoError(t, err)
				}()
			}
			wg.Wait()
			require.NoError(t, client.Close())
		})
	}
	require.NoError(t, errors.Join(listener.Close(), zstdListener.Close()))
}

func makeSeriesBatch() []prompb.TimeSeries {
	series := make([]prompb.TimeSeries, 0, 1000)
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			for k := 0; k < 10; k++ {
				series = append(series, prompb.TimeSeries{
					Labels: []labelpb.ZLabel{{
						Name:  "cluster",
						Value: strconv.Itoa(j),
					}, {
						Name:  "pod",
						Value: strconv.Itoa(k),
					}, {
						Name:  "series",
						Value: strconv.Itoa(i),
					}},
					Samples: []prompb.Sample{
						{Value: 1, Timestamp: 2},
					},
				})
			}
		}
	}
	return series
}
