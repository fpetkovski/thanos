// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"

	"capnproto.org/go/capnp/v3"
	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestCapNProtoServer_SingleConcurrentClient(t *testing.T) {
	custom.TolerantVerifyLeak(t)
	var (
		logger = log.NewNopLogger()
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener = bufconn.Listen(1024)
		handler  = NewCapNProtoHandler(logger, writer)
		srv      = NewCapNProtoServer(listener, handler, logger)
	)
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown()

	for i := 0; i < 1000; i++ {
		client := writecapnp.NewRemoteWriteClient(listener, logger)
		_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
			Tenant:     "default",
			Timeseries: makeTimeSeries(10, 10, 10),
		})
		require.NoError(t, err)
		require.NoError(t, client.Close())
	}
	require.NoError(t, listener.Close())
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
		listener = bufconn.Listen(1024)
		handler  = NewCapNProtoHandler(logger, writer)
		srv      = NewCapNProtoServer(listener, handler, logger)
	)
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown()

	client := writecapnp.NewRemoteWriteClient(listener, logger)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
				Tenant:     "default",
				Timeseries: makeTimeSeries(10, 10, 10),
			})
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	require.NoError(t, client.Close())
	require.NoError(t, listener.Close())
}

func TestCapNProtoServer_MultipleConcurrentClients(t *testing.T) {
	custom.TolerantVerifyLeak(t)
	var (
		logger = log.NewNopLogger()
		writer = NewCapNProtoWriter(
			logger,
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener = bufconn.Listen(1024)
		handler  = NewCapNProtoHandler(logger, writer)
		srv      = NewCapNProtoServer(listener, handler, logger)
	)
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown()

	for i := 0; i < 1000; i++ {
		client := writecapnp.NewRemoteWriteClient(newFlakyConnDialer(listener), logger)
		_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
			Tenant:     "default",
			Timeseries: makeTimeSeries(10, 10, 10),
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, client.Close())
		}()
	}

	require.NoError(t, listener.Close())
}

func makeTimeSeries(numSeries int, numClusters int, numPods int) []prompb.TimeSeries {
	series := make([]prompb.TimeSeries, 0, numSeries*numClusters*numPods)
	for i := 0; i < numSeries; i++ {
		for j := 0; j < numClusters; j++ {
			for k := 0; k < numPods; k++ {
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

type flakyConnDialer struct {
	i      *int
	dialer writecapnp.Dialer
}

func newFlakyConnDialer(dialer writecapnp.Dialer) *flakyConnDialer {
	var i int
	return &flakyConnDialer{
		i:      &i,
		dialer: dialer,
	}
}

func (d flakyConnDialer) Dial() (net.Conn, error) {
	conn, err := d.dialer.Dial()
	if err != nil {
		return nil, err
	}
	return newFlakyConnection(conn, d.i), nil
}

type flakyConnection struct {
	net.Conn
	i *int
}

func newFlakyConnection(conn net.Conn, i *int) *flakyConnection {
	return &flakyConnection{
		i:    i,
		Conn: conn,
	}
}

func (n *flakyConnection) Write(b []byte) (int, error) {
	*n.i++
	if *n.i == 3 || *n.i == 6 {
		return 0, capnp.Disconnected("failed write")
	}
	return n.Conn.Write(b)
}
