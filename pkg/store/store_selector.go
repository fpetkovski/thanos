// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const (
	reMatchAny = "^$"
)

var (
	SupportedRelabelActions = map[relabel.Action]struct{}{
		relabel.Keep:      {},
		relabel.Drop:      {},
		relabel.HashMod:   {},
		relabel.LabelDrop: {},
		relabel.LabelKeep: {},
	}
)

type storeSelector struct {
	relabelConfig []*relabel.Config
}

func newStoreSelector(relabelConfig []*relabel.Config) *storeSelector {
	return &storeSelector{
		relabelConfig: relabelConfig,
	}
}

func (sr *storeSelector) matchStores(stores []Client, replicaLabels []string) ([]Client, []storepb.LabelMatcher) {
	if sr.relabelConfig == nil {
		return stores, nil
	}

	replicaLabelSet := make(map[string]struct{})
	for _, lbl := range replicaLabels {
		replicaLabelSet[lbl] = struct{}{}
	}

	propagatedMatchers := make([]labels.Labels, 0)
	matchingStores := make([]Client, 0)
	for _, st := range stores {
		storeLabelSets := labelsWithoutReplicaLabels(st, replicaLabelSet)
		relabeled := sr.runRelabelRules(storeLabelSets)
		if len(relabeled) == 0 {
			continue
		}

		matchingStores = append(matchingStores, st)
		propagatedMatchers = append(propagatedMatchers, relabeled...)
	}

	return matchingStores, sr.buildLabelMatchers(propagatedMatchers)
}

func labelsWithoutReplicaLabels(st Client, replicaLabelSet map[string]struct{}) []labels.Labels {
	lsets := make([]labels.Labels, 0)
	for _, lset := range st.LabelSets() {
		lsets = append(lsets, rmLabels(lset, replicaLabelSet))
	}
	return lsets
}

func (sr *storeSelector) runRelabelRules(labelSets []labels.Labels) []labels.Labels {
	result := make([]labels.Labels, 0)
	for _, labelSet := range labelSets {
		if _, keep := relabel.Process(labelSet, sr.relabelConfig...); !keep {
			continue
		}

		result = append(result, labelSet)
	}

	return result
}

func (sr *storeSelector) buildLabelMatchers(labelSets []labels.Labels) []storepb.LabelMatcher {
	labelCounts := make(map[string]int)
	matcherSet := make(map[string]map[string]struct{})
	for _, labelSet := range labelSets {
		labelSet.Range(func(lbl labels.Label) {
			if _, ok := matcherSet[lbl.Name]; !ok {
				matcherSet[lbl.Name] = make(map[string]struct{})
			}
			labelCounts[lbl.Name]++
			matcherSet[lbl.Name][lbl.Value] = struct{}{}
		})
	}

	for k := range matcherSet {
		if labelCounts[k] < len(labelSets) {
			matcherSet[k][reMatchAny] = struct{}{}
		}
	}

	matchers := make([]storepb.LabelMatcher, 0, len(matcherSet))
	for k, v := range matcherSet {
		var matchedValues []string
		for val := range v {
			matchedValues = append(matchedValues, val)
		}

		sort.Strings(matchedValues)
		var matcher storepb.LabelMatcher
		if len(matchedValues) == 1 {
			matcher = storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: k, Value: matchedValues[0]}
		} else {
			matcher = storepb.LabelMatcher{Type: storepb.LabelMatcher_RE, Name: k, Value: strings.Join(matchedValues, "|")}
		}
		matchers = append(matchers, matcher)
	}

	return matchers
}
