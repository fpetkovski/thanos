// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net"
	"sync"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
)

type CapNProtoServer struct {
	mu       sync.Mutex
	listener net.Listener
}

func NewCapNProtoServer() *CapNProtoServer {
	return &CapNProtoServer{}
}

func (c *CapNProtoServer) ListenAndServe(addr string, client capnp.Client) error {
	if err := c.connect(addr); err != nil {
		return err
	}
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer conn.Close()
			rpcConn := rpc.NewConn(rpc.NewPackedStreamTransport(conn), &rpc.Options{
				// The BootstrapClient is the RPC interface that will be made available
				// to the remote endpoint by default.
				BootstrapClient: client.AddRef(),
			})
			<-rpcConn.Done()
		}()
	}
}

func (c *CapNProtoServer) connect(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	c.listener, err = net.Listen("tcp", addr)
	return err
}

func (c *CapNProtoServer) Shutdown() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listener != nil {
		return c.listener.Close()
	}
	return nil
}

type CapNProtoHandler struct {
	writer *Writer
	logger log.Logger
}

func NewCapNProtoHandler(logger log.Logger, writer *Writer) *CapNProtoHandler {
	return &CapNProtoHandler{logger: logger, writer: writer}
}

func (c CapNProtoHandler) Write(ctx context.Context, call writecapnp.Writer_write) error {
	wr, err := call.Args().Wr()
	if err != nil {
		return err
	}
	t, err := wr.Tenant()
	if err != nil {
		return err
	}

	var errs writeErrors
	errs.Add(c.writer.Write(ctx, t, writecapnp.NewWriteableRequest(wr), false))
	if err := errs.ErrOrNil(); err != nil {
		level.Debug(c.logger).Log("msg", "failed to handle request", "err", err)
		result, allocErr := call.AllocResults()
		if allocErr != nil {
			return allocErr
		}

		switch errors.Cause(err) {
		case nil:
			return nil
		case errNotReady:
			result.SetError(writecapnp.WriteError_unavailable)
		case errUnavailable:
			result.SetError(writecapnp.WriteError_unavailable)
		case errConflict:
			result.SetError(writecapnp.WriteError_alreadyExists)
		case errBadReplica:
			result.SetError(writecapnp.WriteError_invalidArgument)
		default:
			result.SetError(writecapnp.WriteError_internal)
		}
	}
	return nil
}
