package querysharding_test

import (
	"context"
	"io"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queryClient struct {
	querypb.Query_QueryClient

	i         int
	responses []*querypb.QueryResponse
}

func newQueryClient(series []*prompb.TimeSeries) *queryClient {
	responses := make([]*querypb.QueryResponse, len(series))
	for i := range series {
		responses[i] = querypb.NewQueryResponse(series[i])
	}
	return &queryClient{
		responses: responses,
	}
}

func (q *queryClient) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.Query_QueryClient, error) {
	return q, nil
}

func (q *queryClient) QueryRange(ctx context.Context, in *querypb.QueryRangeRequest, opts ...grpc.CallOption) (querypb.Query_QueryRangeClient, error) {
	return nil, nil
}

func (q *queryClient) Recv() (*querypb.QueryResponse, error) {
	if q.i == len(q.responses) {
		return nil, io.EOF
	}
	defer func() {
		q.i++
	}()

	return q.responses[q.i], nil
}

type cancellableQueryClient struct {
	querypb.Query_QueryClient

	ctx context.Context
}

func newCancellableClient(ctx context.Context) *cancellableQueryClient {
	return &cancellableQueryClient{
		ctx: ctx,
	}
}

func (q *cancellableQueryClient) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.Query_QueryClient, error) {
	return q, nil
}

func (q *cancellableQueryClient) QueryRange(ctx context.Context, in *querypb.QueryRangeRequest, opts ...grpc.CallOption) (querypb.Query_QueryRangeClient, error) {
	return nil, nil
}

func (q *cancellableQueryClient) Recv() (*querypb.QueryResponse, error) {
	<-q.ctx.Done()
	return nil, status.Error(codes.Canceled, "cancelled")
}

type queryRangeClient struct {
	querypb.Query_QueryRangeClient

	i         int
	responses []*querypb.QueryRangeResponse
}

func newQueryRangeClient(series []*prompb.TimeSeries) *queryRangeClient {
	responses := make([]*querypb.QueryRangeResponse, len(series))
	for i := range series {
		responses[i] = querypb.NewQueryRangeResponse(series[i])
	}
	return &queryRangeClient{
		responses: responses,
	}
}

func (q *queryRangeClient) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.Query_QueryClient, error) {
	return nil, nil
}

func (q *queryRangeClient) QueryRange(ctx context.Context, in *querypb.QueryRangeRequest, opts ...grpc.CallOption) (querypb.Query_QueryRangeClient, error) {
	return q, nil
}

func (q *queryRangeClient) Recv() (*querypb.QueryRangeResponse, error) {
	if q.i == len(q.responses) {
		return nil, io.EOF
	}
	defer func() {
		q.i++
	}()

	return q.responses[q.i], nil
}

type recvFunc func() (*querypb.QueryRangeResponse, error)

type queryRangeClientStub struct {
	querypb.Query_QueryRangeClient

	recv recvFunc
}

func newQueryRangeClientStub(recv recvFunc) *queryRangeClientStub {
	return &queryRangeClientStub{
		recv: recv,
	}
}

func (q *queryRangeClientStub) Recv() (*querypb.QueryRangeResponse, error) {
	return q.recv()
}

func (q *queryRangeClientStub) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.Query_QueryClient, error) {
	return nil, nil
}

func (q *queryRangeClientStub) QueryRange(ctx context.Context, in *querypb.QueryRangeRequest, opts ...grpc.CallOption) (querypb.Query_QueryRangeClient, error) {
	return q, nil
}
