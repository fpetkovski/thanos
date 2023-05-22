// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sort"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"

	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
)

func TestStoreSelector(t *testing.T) {
	testCases := []struct {
		name          string
		relabelConfig []*relabel.Config
		stores        []Client
		replicaLabels []string

		expectedMatchers []storepb.LabelMatcher
		expectedStores   []string
	}{
		{
			name:          "no relabel config",
			relabelConfig: nil,
			stores: []Client{
				storeClient(
					"store-1",
					labels.FromStrings("cluster", "a"),
					labels.FromStrings("cluster", "b")),
			},
			expectedMatchers: nil,
			expectedStores:   []string{"store-1"},
		},
		{
			name: "1 matching TSDB against 1 store with 2 TSDBs",
			stores: []Client{
				storeClient(
					"store-1",
					labels.FromStrings("cluster", "a"),
					labels.FromStrings("cluster", "b")),
			},
			relabelConfig: relabelKeep("cluster", "a"),
			expectedMatchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "cluster", Value: "a"},
			},
			expectedStores: []string{"store-1"},
		},
		{
			name: "1 matching TSDB against 2 stores with 1 TSDB",
			stores: []Client{
				storeClient("store-1", labels.FromStrings("ext", "1", "cluster", "a")),
				storeClient("store-2", labels.FromStrings("cluster", "b")),
			},
			relabelConfig: relabelKeep("cluster", "a"),
			expectedMatchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "cluster", Value: "a"},
				{Type: storepb.LabelMatcher_EQ, Name: "ext", Value: "1"},
			},
			expectedStores: []string{"store-1"},
		},
		{
			name: "1 matching TSDB against 1 stores with 1 TSDBs with common external labels",
			stores: []Client{
				storeClient("store-1", labels.FromStrings("ext", "1", "cluster", "a")),
				storeClient("store-2", labels.FromStrings("ext", "1", "cluster", "b")),
			},
			relabelConfig: relabelKeep("cluster", "a"),
			expectedMatchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "cluster", Value: "a"},
				{Type: storepb.LabelMatcher_EQ, Name: "ext", Value: "1"},
			},
			expectedStores: []string{"store-1"},
		},
		{
			name: "2 matching TSDB against 2 stores with 1 TSDBs",
			stores: []Client{
				storeClient("store-1", labels.FromStrings("ext", "1", "cluster", "a")),
				storeClient("store-2", labels.FromStrings("ext", "2", "cluster", "a")),
			},
			relabelConfig: relabelKeep("cluster", "a"),
			expectedMatchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "cluster", Value: "a"},
				{Type: storepb.LabelMatcher_RE, Name: "ext", Value: "1|2"},
			},
			expectedStores: []string{"store-1", "store-2"},
		},
		{
			name: "2 matching TSDB against 2 stores with 1 TSDBs and different number of external labels",
			stores: []Client{
				storeClient("store-1", labels.FromStrings("ext", "1", "cluster", "a")),
				storeClient("store-2", labels.FromStrings("cluster", "a")),
				storeClient("store-3", labels.FromStrings("cluster", "b")),
			},
			relabelConfig: relabelKeep("cluster", "a"),
			expectedMatchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "cluster", Value: "a"},
				{Type: storepb.LabelMatcher_RE, Name: "ext", Value: "1|^$"},
			},
			expectedStores: []string{"store-1", "store-2"},
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.name, func(t *testing.T) {
			selector := newStoreSelector(tcase.relabelConfig)

			stores, matchers := selector.matchStores(tcase.stores, tcase.replicaLabels)
			sort.Slice(matchers, func(i, j int) bool {
				return matchers[i].Name < matchers[j].Name
			})
			testutil.Assert(t, len(stores) == len(tcase.expectedStores), "expected %d stores, got %d", len(tcase.expectedStores), len(stores))
			testutil.Equals(t, tcase.expectedMatchers, matchers)
		})
	}
}

// nolint:unparam
func relabelKeep(labelName string, labelValue string) []*relabel.Config {
	return []*relabel.Config{
		{
			SourceLabels: model.LabelNames{model.LabelName(labelName)},
			Regex:        relabel.MustNewRegexp(labelValue),
			Action:       relabel.Keep,
		},
	}
}

func storeClient(storeName string, storeLabelSets ...labels.Labels) Client {
	return storetestutil.TestClient{
		Name:    storeName,
		ExtLset: storeLabelSets,
	}
}
