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

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type Dialer interface {
	Dial() (net.Conn, error)
}

type TCPDialer struct {
	address string
}

func NewTCPDialer(address string) *TCPDialer {
	return &TCPDialer{address: address}
}

func (t TCPDialer) Dial() (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", t.address)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial peer %s", t.address)
	}
	return conn, nil
}

type RemoteWriteClient struct {
	mu sync.Mutex

	dialer Dialer
	conn   *net.Conn

	writer Writer
}

func NewRemoteWriteClient(dialer Dialer) *RemoteWriteClient {
	return &RemoteWriteClient{
		dialer: dialer,
	}
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
			if err := r.Close(); err != nil {
				return nil, err
			}
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

	conn, err := r.dialer.Dial()
	if err != nil {
		return errors.Wrap(err, "failed to dial peer")
	}
	r.writer = Writer(rpc.NewConn(rpc.NewPackedStreamTransport(conn), nil).Bootstrap(ctx))
	r.conn = &conn
	return nil
}

func (r *RemoteWriteClient) Close() error {
	r.mu.Lock()
	if r.conn != nil {
		r.writer.Release()

		conn := *r.conn
		r.conn = nil
		go conn.Close()
	}
	r.mu.Unlock()
	return nil
}
