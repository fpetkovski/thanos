package storepb

import (
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func (m *ShardInfo) MatchesSeries(zLabels []labelpb.ZLabel, groupingBy bool, groupingLabelset map[string]struct{}) bool {
	if !m.IsSharded() {
		return true
	}

	var shardLabels []labelpb.ZLabel
	for _, zl := range zLabels {
		if zl.Name == "__name__" {
			continue
		}

		if includeLabel(zl, groupingBy, groupingLabelset) {
			shardLabels = append(shardLabels, zl)
		}
	}

	promLabels := labelpb.ZLabelsToPromLabels(shardLabels)
	return promLabels.Hash()%uint64(m.TotalShards) == uint64(m.ShardIndex)
}

func (m *ShardInfo) IsSharded() bool {
	return m != nil && m.TotalShards > 1
}

func includeLabel(zlabel labelpb.ZLabel, groupingBy bool, groupingLabelSet map[string]struct{}) bool {
	if len(groupingLabelSet) == 0 {
		return true
	}

	_, labelFound := groupingLabelSet[zlabel.Name]
	if groupingBy && !labelFound {
		return false
	}

	if !groupingBy && labelFound {
		return false
	}

	return true
}
