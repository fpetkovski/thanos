// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package containing proto and JSON serializable Labels and ZLabels (no copy) structs used to
// identify series. This package expose no-copy converters to Prometheus labels.Labels.

//go:build !stringlabels && !dedupelabels

package labelpb

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
)

// ZLabelsFromPromLabels converts Prometheus labels to slice of labelpb.ZLabel in type unsafe manner.
// It reuses the same memory. Caller should abort using passed labels.Labels.
func ZLabelsFromPromLabels(lset labels.Labels) []ZLabel {
	return *(*[]ZLabel)(unsafe.Pointer(&lset))
}

// ZLabelsToPromLabels convert slice of labelpb.ZLabel to Prometheus labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed []ZLabel.
// NOTE: Use with care. ZLabels holds memory from the whole protobuf unmarshal, so the returned
// Prometheus Labels will hold this memory as well.
func ZLabelsToPromLabels(lset []ZLabel) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&lset))
}

// ZLabelSetsToPromLabelSets converts slice of labelpb.ZLabelSet to slice of Prometheus labels.
func ZLabelSetsToPromLabelSets(lss ...ZLabelSet) []labels.Labels {
	res := make([]labels.Labels, 0, len(lss))
	for _, ls := range lss {
		res = append(res, ls.PromLabels())
	}
	return res
}

// ZLabelSetsFromPromLabels converts []labels.labels to []labelpb.ZLabelSet.
func ZLabelSetsFromPromLabels(lss ...labels.Labels) []ZLabelSet {
	sets := make([]ZLabelSet, 0, len(lss))
	for _, ls := range lss {
		set := ZLabelSet{
			Labels: make([]ZLabel, 0, ls.Len()),
		}
		ls.Range(func(lbl labels.Label) {
			set.Labels = append(set.Labels, ZLabel{
				Name:  lbl.Name,
				Value: lbl.Value,
			})
		})
		sets = append(sets, set)
	}

	return sets
}
