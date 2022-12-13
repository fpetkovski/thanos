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

const projectIDLabel = "project_id"

type StackdriverStore struct {
}

func NewStackdriverStore() *StackdriverStore {
	return &StackdriverStore{}
}

func (store StackdriverStore) Info(ctx context.Context, request *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	return &storepb.InfoResponse{
		MinTime:   math.MinInt64,
		MaxTime:   math.MaxInt64,
		StoreType: storepb.StoreType_STORE,
	}, nil
}

func (store StackdriverStore) Series(request *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	metricsClient, err := monitoring.NewMetricClient(server.Context())
	if err != nil {
		return err
	}

	params, err := getStackdriverParameters(request)
	if err != nil {
		return err
	}

	fmt.Println("Filters", params.filters)
	fmt.Println("Project", params.projectName)

	it := metricsClient.ListTimeSeries(server.Context(), &monitoringpb.ListTimeSeriesRequest{
		Name:     "projects/" + params.projectName,
		Filter:   strings.Join(params.filters, " AND "),
		PageSize: 10000000,
		Aggregation: &monitoringpb.Aggregation{
			AlignmentPeriod: &durationpb.Duration{
				Seconds: 30 * 4,
			},
			CrossSeriesReducer: monitoringpb.Aggregation_REDUCE_SUM,
			PerSeriesAligner:   monitoringpb.Aggregation_ALIGN_RATE,
			GroupByFields:      []string{"resource.labels.cluster_name"},
		},
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(time.UnixMilli(request.MinTime)),
			EndTime:   timestamppb.New(time.UnixMilli(request.MaxTime)),
		},
	})

	for {
		start := time.Now()
		timeSeries, err := it.Next()
		fmt.Println("Got series in", time.Since(start))
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}

		start = time.Now()
		fmt.Println("Sending series", timeSeries.Metric.Labels, timeSeries.Resource.Labels, len(timeSeries.Points))
		if err := store.sendResponse(server, timeSeries); err != nil {
			return err
		}
		fmt.Println("Sent series in", time.Since(start), "seconds")
	}
}

func (store *StackdriverStore) sendResponse(
	server storepb.Store_SeriesServer,
	timeSeries *monitoringpb.TimeSeries,
) error {
	lbls := make([]labelpb.ZLabel, 0, len(timeSeries.Metric.Labels)+len(timeSeries.Resource.Labels))
	for name, value := range timeSeries.Metric.Labels {
		lbls = append(lbls, labelpb.ZLabel{
			Name:  "metric_" + name,
			Value: value,
		})
	}
	for name, value := range timeSeries.Resource.Labels {
		lbls = append(lbls, labelpb.ZLabel{
			Name:  "resource_" + name,
			Value: value,
		})
	}

	c := chunkenc.NewXORChunk()
	a, err := c.Appender()
	if err != nil {
		return err
	}

	var minTime int64 = math.MaxInt64
	var maxTime int64 = math.MinInt64
	n := len(timeSeries.Points)
	for i := range timeSeries.Points {
		point := timeSeries.Points[n-i-1]
		if point.Interval.StartTime.GetSeconds() < minTime {
			minTime = point.Interval.StartTime.GetSeconds()
		}
		if point.Interval.EndTime.GetSeconds() > maxTime {
			maxTime = point.Interval.EndTime.GetSeconds()
		}
		a.Append(point.Interval.EndTime.GetSeconds()*1000, point.Value.GetDoubleValue())
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

func getStackdriverParameters(request *storepb.SeriesRequest) (stackdriverQueryParams, error) {
	var params stackdriverQueryParams

	for _, m := range request.Matchers {
		switch m.Name {
		case labels.MetricName:
			params.metricName = m.Value
		case projectIDLabel:
			params.projectName = m.Value
		default:
			if strings.HasPrefix(m.Name, "resource_") {
				m.Name = "resource.labels." + m.Name[len("resource_"):]
			} else if strings.HasPrefix(m.Name, "metric_") {
				m.Name = "metric.labels." + m.Name[len("metric_"):]
			}

			params.filters = append(params.filters, matcherToStackdriverFilter(m))
		}
	}

	if params.projectName == "" {
		return stackdriverQueryParams{}, errors.New("project_id label has to be specified")
	}
	if params.metricName == "" {
		return stackdriverQueryParams{}, errors.New("metric name has to be specified")
	}
	params.filters = append(params.filters, matcherToStackdriverFilter(storepb.LabelMatcher{
		Type:  storepb.LabelMatcher_EQ,
		Name:  "metric.type",
		Value: params.metricName,
	}))

	params.queryRange = (request.MaxTime - request.MinTime) / 1000
	return params, nil
}

func matcherToStackdriverFilter(m storepb.LabelMatcher) string {
	var matcherType string
	switch m.Type {
	case storepb.LabelMatcher_EQ:
		matcherType = "="
	case storepb.LabelMatcher_NEQ:
		matcherType = "!="
	case storepb.LabelMatcher_RE:
		matcherType = "=~"
	case storepb.LabelMatcher_NRE:
		matcherType = "!~"
	}

	return fmt.Sprintf(`%s %s "%s"`, m.Name, matcherType, m.Value)
}

func (store StackdriverStore) LabelNames(ctx context.Context, request *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return &storepb.LabelNamesResponse{}, nil
}

func (store StackdriverStore) LabelValues(ctx context.Context, request *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{}, nil
}
