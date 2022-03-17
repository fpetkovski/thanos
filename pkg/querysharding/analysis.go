package querysharding

type QueryAnalysis struct {
	// Labels to shard on
	shardingLabels []string

	// When set to true, sharding is `by` shardingLabels,
	// otherwise it is `without` shardingLabels.
	shardBy bool
}

func nonShardableQuery() QueryAnalysis {
	return QueryAnalysis{
		shardingLabels: nil,
	}
}

func newShardableByLabels(labels []string, by bool) QueryAnalysis {
	return QueryAnalysis{
		shardBy:        by,
		shardingLabels: labels,
	}
}

func (q QueryAnalysis) scopeToLabels(labels []string, by bool) QueryAnalysis {
	if q.shardingLabels == nil {
		return QueryAnalysis{
			shardBy:        by,
			shardingLabels: labels,
		}
	}

	if by {
		return QueryAnalysis{
			shardBy:        true,
			shardingLabels: intersect(q.shardingLabels, labels),
		}
	}

	return QueryAnalysis{
		shardBy:        false,
		shardingLabels: union(q.shardingLabels, labels),
	}
}

func (q QueryAnalysis) IsShardable() bool {
	return len(q.shardingLabels) > 0
}

func (q QueryAnalysis) ShardingLabels() []string {
	if len(q.shardingLabels) == 0 {
		return nil
	}

	return q.shardingLabels
}

func (q QueryAnalysis) ShardBy() bool {
	return q.shardBy
}

func intersect(sliceA, sliceB []string) []string {
	if len(sliceA) == 0 || len(sliceB) == 0 {
		return []string{}
	}

	mapA := make(map[string]struct{}, len(sliceA))
	for _, s := range sliceA {
		mapA[s] = struct{}{}
	}

	mapB := make(map[string]struct{}, len(sliceB))
	for _, s := range sliceB {
		mapB[s] = struct{}{}
	}

	result := make([]string, 0)
	for k, _ := range mapA {
		if _, ok := mapB[k]; ok {
			result = append(result, k)
		}
	}

	return result
}

func union(sliceA, sliceB []string) []string {
	if len(sliceA) == 0 || len(sliceB) == 0 {
		return []string{}
	}

	keyMap := make(map[string]struct{}, len(sliceA))
	for _, s := range sliceA {
		keyMap[s] = struct{}{}
	}
	for _, s := range sliceB {
		keyMap[s] = struct{}{}
	}

	result := make([]string, 0, len(keyMap))
	for k, _ := range keyMap {
		result = append(result, k)
	}

	return result
}
