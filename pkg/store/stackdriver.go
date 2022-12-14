package store

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"google.golang.org/api/iterator"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const (
	projectIDLabel    = "project_id"
	metricLabelPrefix = "metric:"
)

type stackdriverStore struct {
	mu *sync.RWMutex

	// metricNamesMap is a map from Prometheus-compatible metric names
	// to the original metric name in Stackdriver.
	metricNamesMap map[string]string
	// metricDescriptors is a list of all metric descriptors found in Stackdriver.
	metricDescriptors []*metricpb.MetricDescriptor
}

func NewStackdriverStore() *stackdriverStore {
	return &stackdriverStore{
		mu:                &sync.RWMutex{},
		metricNamesMap:    make(map[string]string),
		metricDescriptors: make([]*metricpb.MetricDescriptor, 0),
	}
}

func (sdStore *stackdriverStore) RefreshMetrics(ctx context.Context) error {
	it, err := sdStore.getMetricDescriptors(ctx, nil)
	if err != nil {
		return err
	}

	namesMap := make(map[string]string)
	metrics := make([]*metricpb.MetricDescriptor, 0)
	for {
		descriptor, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		metrics = append(metrics, descriptor)
		promName := strings.ReplaceAll(strings.ReplaceAll(descriptor.Type, ".", ":"), "/", "_")
		namesMap[promName] = descriptor.Type
		descriptor.Type = promName
	}

	sdStore.mu.Lock()
	sdStore.metricDescriptors = metrics
	sdStore.metricNamesMap = namesMap
	sdStore.mu.Unlock()

	return nil
}

func (sdStore stackdriverStore) Info(ctx context.Context, request *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	return &storepb.InfoResponse{
		MinTime:   math.MinInt64,
		MaxTime:   math.MaxInt64,
		StoreType: storepb.StoreType_STORE,
	}, nil
}

func (sdStore stackdriverStore) Series(request *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	metricsClient, err := monitoring.NewMetricClient(server.Context())
	if err != nil {
		return err
	}

	if request.SkipChunks {
		return sdStore.rawSeries(request, server)
	}

	params, err := sdStore.getQueryParams(request.Matchers)
	if err != nil {
		return err
	}
	if params.metricName == "" {
		return errors.New("metric name has to be specified")
	}
	if params.projectName == "" {
		return errors.New("project_id label has to be specified")
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

func (sdStore *stackdriverStore) sendResponse(
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

	sort.Slice(lbls, func(i, j int) bool {
		return lbls[i].Name < lbls[j].Name
	})

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

func (sdStore *stackdriverStore) rawSeries(request *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	params, err := sdStore.getQueryParams(request.Matchers)
	if err != nil {
		return err
	}
	if params.metricName == "" {
		return errors.New("metric name has to be specified")
	}
	if params.projectName == "" {
		params.projectName = "shopify-observability-prom"
	}

	sdStore.mu.RLock()
	defer sdStore.mu.RUnlock()

	metrics, err := sdStore.getMetricDescriptors(server.Context(), request.Matchers)
	if err != nil {
		return err
	}

	resourceClient, err := monitoring.NewMetricClient(server.Context())
	if err != nil {
		return err
	}

	var once sync.Once
	for {
		it, err := metrics.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}

		once.Do(func() {
			if len(it.MonitoredResourceTypes) == 0 {
				return
			}

			resources := resourceClient.ListMonitoredResourceDescriptors(server.Context(), &monitoringpb.ListMonitoredResourceDescriptorsRequest{
				Name:   "projects/" + params.projectName,
				Filter: "resource.type=" + it.MonitoredResourceTypes[0],
			})
			for {
				it, err := resources.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return
				}
				for _, lbl := range it.Labels {
					if err := server.Send(storepb.NewSeriesResponse(&storepb.Series{
						Labels: []labelpb.ZLabel{{
							Name:  labels.MetricName,
							Value: params.metricName,
						}, {
							Name:  lbl.Key,
							Value: "_",
						}},
					})); err != nil {
						continue
					}
				}
			}
		})

		for _, lbl := range it.Labels {
			if err := server.Send(storepb.NewSeriesResponse(&storepb.Series{
				Labels: []labelpb.ZLabel{{
					Name:  labels.MetricName,
					Value: params.metricName,
				}, {
					Name:  lbl.Key,
					Value: "_",
				}},
			})); err != nil {
				return err
			}
		}
	}
}

type stackdriverQueryParams struct {
	projectName string
	metricName  string
	filters     []string
}

func (sdStore *stackdriverStore) getQueryParams(matchers []storepb.LabelMatcher) (stackdriverQueryParams, error) {
	var params stackdriverQueryParams

	for _, m := range matchers {
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

	if params.metricName != "" {
		sdStore.mu.RLock()
		stackdriverMetricName, ok := sdStore.metricNamesMap[params.metricName]
		sdStore.mu.RUnlock()
		if !ok {
			stackdriverMetricName = params.metricName
		}

		metricMatcher := storepb.LabelMatcher{
			Type:  storepb.LabelMatcher_EQ,
			Name:  "metric.type",
			Value: stackdriverMetricName,
		}
		params.filters = append(params.filters, metricMatcher.PromString())
	}

	return params, nil
}

func (sdStore *stackdriverStore) getAggregation(hints *storepb.QueryHints) *monitoringpb.Aggregation {
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
		PerSeriesAligner:   alignerFromHints(hints),
		GroupByFields:      lbls,
	}
}

func (sdStore *stackdriverStore) mapLabelName(lbl string) string {
	if strings.HasPrefix(lbl, metricLabelPrefix) {
		return "metric.labels." + lbl[len(metricLabelPrefix):]
	}

	return "resource.labels." + lbl
}

func alignerFromHints(hints *storepb.QueryHints) monitoringpb.Aggregation_Aligner {
	if hints == nil || (hints.TimeFunc == nil && hints.AggrFunc == nil) {
		return monitoringpb.Aggregation_ALIGN_NONE
	}
	if hints.TimeFunc == nil && hints.AggrFunc != nil {
		return monitoringpb.Aggregation_ALIGN_MEAN
	}

	switch hints.TimeFunc.Name {
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

func (sdStore stackdriverStore) LabelNames(ctx context.Context, request *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	sdStore.mu.RLock()
	defer sdStore.mu.RUnlock()

	lset := make(map[string]struct{})
	for _, metric := range sdStore.metricDescriptors {
		for _, lbl := range metric.Labels {
			lset[lbl.Key] = struct{}{}
		}
	}

	lbls := make([]string, 0, len(lset))
	for lbl := range lset {
		lbls = append(lbls, lbl)
	}
	sort.Strings(lbls)
	return &storepb.LabelNamesResponse{
		Names: lbls,
	}, nil
}

func (sdStore stackdriverStore) LabelValues(ctx context.Context, request *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	sdStore.mu.RLock()
	defer sdStore.mu.RUnlock()

	valuesMap := make(map[string]struct{})
	lset := make(map[string]struct{})
	for _, metric := range sdStore.metricDescriptors {
		for _, lbl := range metric.Labels {
			lset[lbl.Key] = struct{}{}
		}

		if request.Label == labels.MetricName {
			valuesMap[metric.Type] = struct{}{}
			continue
		}

		for _, lbl := range metric.Labels {
			if lbl.Key == request.Label {
				valuesMap[lbl.Key] = struct{}{}
			}
		}
	}

	values := make([]string, 0, len(valuesMap))
	for value := range valuesMap {
		values = append(values, value)
	}

	return &storepb.LabelValuesResponse{
		Values: values,
	}, nil
}

func (sdStore *stackdriverStore) getMetricDescriptors(ctx context.Context, matchers []storepb.LabelMatcher) (*monitoring.MetricDescriptorIterator, error) {
	metricsClient, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		return nil, err
	}

	params, err := sdStore.getQueryParams(matchers)
	if err != nil {
		return nil, err
	}
	if params.projectName == "" {
		params.projectName = "shopify-observability-prom"
	}

	return metricsClient.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
		Name:   "projects/" + params.projectName,
		Filter: strings.Join(params.filters, " AND "),
	}), nil
}
