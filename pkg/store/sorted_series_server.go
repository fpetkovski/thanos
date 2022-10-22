// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sort"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type sortedSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer

	responses           []*storepb.SeriesResponse
	sortWithoutLabelSet map[string]struct{}
	passThrough         bool
}

func newSortedSeriesServer(upstream storepb.Store_SeriesServer, sortWithoutLabelSet map[string]struct{}, passThrough bool) *sortedSeriesServer {
	return &sortedSeriesServer{
		Store_SeriesServer: upstream,

		passThrough:         passThrough,
		sortWithoutLabelSet: sortWithoutLabelSet,

		// Buffered responses when passThrough is false.
		responses: make([]*storepb.SeriesResponse, 0),
	}
}

func (s *sortedSeriesServer) Send(r *storepb.SeriesResponse) error {
	series := r.GetSeries()
	// No need to buffer warnings and hints.
	if series == nil {
		return s.Store_SeriesServer.Send(r)
	}

	moveLabelsToEnd(series.Labels, s.sortWithoutLabelSet)
	if s.passThrough {
		return s.Store_SeriesServer.Send(r)
	}

	s.responses = append(s.responses, r)
	return nil
}

func (s *sortedSeriesServer) Flush() error {
	if s.passThrough {
		return nil
	}

	if len(s.sortWithoutLabelSet) > 0 {
		sort.Slice(s.responses, func(i, j int) bool {
			return compareResponses(s.responses[i], s.responses[j])
		})
	}

	for _, r := range s.responses {
		if err := s.Store_SeriesServer.Send(r); err != nil {
			return err
		}
	}
	return nil
}

func moveLabelsToEnd(labelSet []labelpb.ZLabel, labelsToMove map[string]struct{}) {
	if len(labelsToMove) == 0 {
		return
	}

	sort.Slice(labelSet, func(i, j int) bool {
		if _, ok := labelsToMove[labelSet[i].Name]; ok {
			return false
		}
		if _, ok := labelsToMove[labelSet[j].Name]; ok {
			return true
		}

		return labelSet[i].Name < labelSet[j].Name
	})
}

func sortRequired(sortWithoutLabels map[string]struct{}, extLabelsMap map[string]struct{}) bool {
	for lbl := range sortWithoutLabels {
		if _, isExtLabel := extLabelsMap[lbl]; !isExtLabel {
			return true
		}
	}

	return false
}
