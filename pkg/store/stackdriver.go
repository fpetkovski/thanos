package store

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const (
	projectIDLabel    = "project_id"
	metricLabelPrefix = "metric:"
)

type StackdriverStore struct {
}

func NewStackdriverStore() *StackdriverStore {
	return &StackdriverStore{}
}

func (sdStore StackdriverStore) Info(ctx context.Context, request *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	return &storepb.InfoResponse{
		MinTime:   math.MinInt64,
		MaxTime:   math.MaxInt64,
		StoreType: storepb.StoreType_STORE,
	}, nil
}

func (sdStore StackdriverStore) Series(request *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	metricsClient, err := monitoring.NewMetricClient(server.Context())
	if err != nil {
		return err
	}

	params, err := sdStore.getStackdriverParameters(request)
	if err != nil {
		return err
	}

	fmt.Println("Filters", params.filters)
	fmt.Println("Project", params.projectName)
	fmt.Println("Hints", request.QueryHints)

	aggregation := sdStore.getAggregation(request.QueryHints)
	it := metricsClient.ListTimeSeries(server.Context(), &monitoringpb.ListTimeSeriesRequest{
		Name:        "projects/" + params.projectName,
		Filter:      strings.Join(params.filters, " AND "),
		Aggregation: aggregation,
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(time.UnixMilli(request.MinTime)),
			EndTime:   timestamppb.New(time.UnixMilli(request.MaxTime)),
		},
	})

	for {
		timeSeries, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}

		if len(timeSeries.Metric.Labels) == 0 {
			timeSeries.Metric.Labels = make(map[string]string)
		}
		timeSeries.Resource.Labels[labels.MetricName] = params.metricName

		if err := sdStore.sendResponse(server, request, aggregation.PerSeriesAligner, timeSeries); err != nil {
			return err
		}
	}
}

func (sdStore *StackdriverStore) sendResponse(
	server storepb.Store_SeriesServer,
	request *storepb.SeriesRequest,
	aligner monitoringpb.Aggregation_Aligner,
	timeSeries *monitoringpb.TimeSeries,
) error {
	lbls := make([]labelpb.ZLabel, 0, len(timeSeries.Metric.Labels)+len(timeSeries.Resource.Labels))
	for name, value := range timeSeries.Metric.Labels {
		lbls = append(lbls, labelpb.ZLabel{
			Name:  metricLabelPrefix + name,
			Value: value,
		})
	}

	for name, value := range timeSeries.Resource.Labels {
		lbls = append(lbls, labelpb.ZLabel{
			Name:  name,
			Value: value,
		})
	}

	c := chunkenc.NewXORChunk()
	a, err := c.Appender()
	if err != nil {
		return err
	}

	var (
		minTime      int64   = math.MaxInt64
		maxTime      int64   = math.MinInt64
		cumVal       float64 = 0
		n                    = len(timeSeries.Points)
		invertRate           = aligner == monitoringpb.Aggregation_ALIGN_RATE
		invertFactor         = float64(request.QueryHints.RangeMillis() / 2000)
	)

	for i := range timeSeries.Points {
		point := timeSeries.Points[n-i-1]
		if point.Interval.StartTime.GetSeconds() < minTime {
			minTime = point.Interval.StartTime.GetSeconds()
		}
		if point.Interval.EndTime.GetSeconds() > maxTime {
			maxTime = point.Interval.EndTime.GetSeconds()
		}

		ts := point.Interval.EndTime.GetSeconds() * 1000
		v := point.Value.GetDoubleValue()
		if invertRate {
			v = v*invertFactor + cumVal
			cumVal = v
		}
		a.Append(ts, v)
	}

	series := &storepb.Series{
		Labels: lbls,
		Chunks: []storepb.AggrChunk{
			{
				MinTime: minTime * 1000,
				MaxTime: maxTime * 1000,
				Raw: &storepb.Chunk{
					Type: storepb.Chunk_XOR,
					Data: c.Bytes(),
				},
			},
		},
	}

	return server.Send(storepb.NewSeriesResponse(series))
}

type stackdriverQueryParams struct {
	projectName string
	metricName  string
	filters     []string
	queryRange  int64
}

func (sdStore *StackdriverStore) getStackdriverParameters(request *storepb.SeriesRequest) (stackdriverQueryParams, error) {
	var params stackdriverQueryParams

	for _, m := range request.Matchers {
		switch m.Name {
		case labels.MetricName:
			params.metricName = m.Value
		case projectIDLabel:
			params.projectName = m.Value
		default:
			m.Name = sdStore.mapLabelName(m.Name)
			params.filters = append(params.filters, m.PromString())
		}
	}

	if params.projectName == "" {
		return stackdriverQueryParams{}, errors.New("project_id label has to be specified")
	}
	if params.metricName == "" {
		return stackdriverQueryParams{}, errors.New("metric name has to be specified")
	}

	metricMatcher := storepb.LabelMatcher{
		Type:  storepb.LabelMatcher_EQ,
		Name:  "metric.type",
		Value: params.metricName,
	}
	params.filters = append(params.filters, metricMatcher.PromString())
	params.queryRange = (request.MaxTime - request.MinTime) / 1000
	return params, nil
}

func (sdStore StackdriverStore) getAggregation(hints *storepb.QueryHints) *monitoringpb.Aggregation {
	if hints == nil {
		return nil
	}

	if hints.Grouping != nil && !hints.Grouping.By {
		return nil
	}

	var lbls []string
	if hints.Grouping != nil {
		for _, lbl := range hints.Grouping.Labels {
			lbls = append(lbls, sdStore.mapLabelName(lbl))
		}
	}

	// Use a default alignment interval of 2 minute.
	var alignmentInterval int64 = 120
	if hints.TimeFunc != nil {
		// Align samples using half of the range interval so that
		// rate/increase in PromQL can have two input samples.
		alignmentInterval = hints.RangeMillis() / 1000 / 2
	}

	return &monitoringpb.Aggregation{
		AlignmentPeriod: &durationpb.Duration{
			Seconds: alignmentInterval,
		},
		CrossSeriesReducer: reducerFromAggrFunc(hints.AggrFunc),
		PerSeriesAligner:   alignerFromTimeFunc(hints.TimeFunc),
		GroupByFields:      lbls,
	}
}

func (sdStore StackdriverStore) mapLabelName(lbl string) string {
	if strings.HasPrefix(lbl, metricLabelPrefix) {
		return "metric.labels." + lbl[len(metricLabelPrefix):]
	}

	return "resource.labels." + lbl
}

func alignerFromTimeFunc(timeFunc *storepb.Func) monitoringpb.Aggregation_Aligner {
	if timeFunc == nil {
		return monitoringpb.Aggregation_ALIGN_MEAN
	}
	switch timeFunc.Name {
	case "rate", "increase", "delta", "idelta", "irate":
		return monitoringpb.Aggregation_ALIGN_RATE
	case "avg_over_time":
		return monitoringpb.Aggregation_ALIGN_MEAN
	case "sum_over_time":
		return monitoringpb.Aggregation_ALIGN_SUM
	case "count_over_time":
		return monitoringpb.Aggregation_ALIGN_COUNT
	default:
		return monitoringpb.Aggregation_ALIGN_MEAN
	}
}

func reducerFromAggrFunc(timeFunc *storepb.Func) monitoringpb.Aggregation_Reducer {
	if timeFunc == nil {
		return monitoringpb.Aggregation_REDUCE_NONE
	}
	switch timeFunc.Name {
	case "sum":
		return monitoringpb.Aggregation_REDUCE_SUM
	case "min":
		return monitoringpb.Aggregation_REDUCE_MIN
	case "max":
		return monitoringpb.Aggregation_REDUCE_MAX
	case "count":
		return monitoringpb.Aggregation_REDUCE_COUNT
	case "avg":
		return monitoringpb.Aggregation_REDUCE_MEAN
	default:
		return monitoringpb.Aggregation_REDUCE_NONE
	}
}

func (sdStore StackdriverStore) LabelNames(ctx context.Context, request *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return &storepb.LabelNamesResponse{}, nil
}

func (sdStore StackdriverStore) LabelValues(ctx context.Context, request *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{}, nil
}
