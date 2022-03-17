package querysharding

import (
	"context"
	"io"
	"sync"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/prometheus/prometheus/promql"
)

type timeSeriesIterator interface {
	NextSeries() (*prompb.TimeSeries, string, error)
}

type fanout struct {
	ctx      context.Context
	logger   log.Logger
	clients  []timeSeriesIterator
	series   chan *prompb.TimeSeries
	warnings chan error
}

type message struct {
	series  *prompb.TimeSeries
	warning string
}

func newFanout(
	ctx context.Context,
	logger log.Logger,
	clients []timeSeriesIterator,
) *fanout {
	return &fanout{
		ctx:      ctx,
		logger:   logger,
		clients:  clients,
		series:   make(chan *prompb.TimeSeries, 10000),
		warnings: make(chan error),
	}
}

func (f *fanout) exec() (series []*prompb.TimeSeries, warnings []error, err error) {
	var wg sync.WaitGroup
	wg.Add(1)
	// Cancelling streamCtx indicates to the mergeSeries goroutine
	// that all series have been read from grpc clients.
	streamCtx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		defer cancel()
		err = f.streamFromClients()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		series, warnings = f.mergeSeries(streamCtx)
	}()

	wg.Wait()
	if err != nil {
		return nil, nil, err
	}

	return series, warnings, err
}

func (f *fanout) streamFromClients() error {
	defer close(f.series)
	defer close(f.warnings)

	var receiveErr error
	var wg sync.WaitGroup
	wg.Add(len(f.clients))
	for _, c := range f.clients {
		go func(c timeSeriesIterator) {
			defer wg.Done()
			err := f.streamFromClient(c)
			if receiveErr == nil {
				receiveErr = err
			}
		}(c)
	}

	wg.Wait()
	return receiveErr
}

func (f *fanout) streamFromClient(client timeSeriesIterator) error {
	for {
		series, w, err := client.NextSeries()
		if status.Code(err) == codes.Canceled {
			return promql.ErrQueryCanceled("query cancelled")
		}

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		if w != "" {
			f.warnings <- errors.New(w)
		}

		if series == nil || len(series.Samples) == 0 {
			continue
		}

		f.series <- series
	}
}

func (f *fanout) mergeSeries(streamCtx context.Context) ([]*prompb.TimeSeries, []error) {
	var result []*prompb.TimeSeries
	var warnings []error

readChannels:
	for {
		select {
		case <-f.ctx.Done():
			return nil, nil
		case warning, ok := <-f.warnings:
			if ok {
				warnings = append(warnings, warning)
			}
		case sample, ok := <-f.series:
			if ok {
				result = append(result, sample)
			}
		case <-streamCtx.Done():
			break readChannels
		}
	}

	// Drain channels
	for warning := range f.warnings {
		warnings = append(warnings, warning)
	}
	for sample := range f.series {
		result = append(result, sample)
	}

	return result, warnings
}
