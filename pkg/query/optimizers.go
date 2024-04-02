package query

import (
	"slices"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"golang.org/x/exp/maps"
)

const SeriesIDColumn = "__series__id"

// SetProjectionLabels is an optimizer that sets the projection labels for all vector selectors.
// If a projection is already set as a matcher, it will be materialized in the selector
// and the matcher will be removed.
// This is useful for sending projections as part of a remote query.
type SetProjectionLabels struct{}

func (s SetProjectionLabels) Optimize(expr logicalplan.Node, _ *query.Options) (logicalplan.Node, annotations.Annotations) {
	var hasProjections bool
	logicalplan.TraverseBottomUp(nil, &expr, func(_ *logicalplan.Node, current *logicalplan.Node) bool {
		switch e := (*current).(type) {
		case *logicalplan.VectorSelector:
			// Check if a projection is already set in the node.
			if e.Projection.Include || len(e.Projection.Labels) > 0 {
				return false
			}
		}
		return false
	})
	if hasProjections {
		return expr, annotations.Annotations{}
	}

	var projection logicalplan.Projection
	return s.optimize(expr, projection)
}

func (s SetProjectionLabels) optimize(expr logicalplan.Node, projection logicalplan.Projection) (logicalplan.Node, annotations.Annotations) {
	var stop bool
	logicalplan.Traverse(&expr, func(current *logicalplan.Node) {
		if stop {
			return
		}
		switch e := (*current).(type) {
		case logicalplan.Deduplicate:
			for i := range e.Expressions {
				e.Expressions[i].Query, _ = s.optimize(e.Expressions[i].Query, logicalplan.Projection{
					Include: projection.Include,
					Labels:  append([]string{}, projection.Labels...),
				})
			}
			return
		case *logicalplan.Aggregation:
			switch e.Op {
			case parser.TOPK, parser.BOTTOMK:
				projection = logicalplan.Projection{}
			default:
				projection = logicalplan.Projection{
					Labels:  append([]string{}, e.Grouping...),
					Include: !e.Without,
				}
			}
			return
		case *logicalplan.FunctionCall:
			switch e.Func.Name {
			case "absent_over_time", "absent", "scalar":
				projection = logicalplan.Projection{Include: true}
			case "label_replace":
				switch projection.Include {
				case true:
					if slices.Contains(projection.Labels, logicalplan.UnsafeUnwrapString(e.Args[1])) {
						projection.Labels = append(projection.Labels, logicalplan.UnsafeUnwrapString(e.Args[3]))
					}
				case false:
					if !slices.Contains(projection.Labels, logicalplan.UnsafeUnwrapString(e.Args[1])) {
						projection.Labels = slices.DeleteFunc(projection.Labels, func(s string) bool {
							return s == logicalplan.UnsafeUnwrapString(e.Args[3])
						})
					}
				}
			}
		case *logicalplan.Binary:
			var highCard, lowCard = e.LHS, e.RHS
			if e.VectorMatching == nil || (!e.VectorMatching.On && len(e.VectorMatching.MatchingLabels) == 0) {
				if logicalplan.IsConstantExpr(lowCard) {
					s.optimize(highCard, projection)
				} else {
					s.optimize(highCard, logicalplan.Projection{})
				}

				if logicalplan.IsConstantExpr(highCard) {
					s.optimize(lowCard, projection)
				} else {
					s.optimize(lowCard, logicalplan.Projection{})
				}
				stop = true
				return
			}
			if e.VectorMatching.Card == parser.CardOneToMany {
				highCard, lowCard = lowCard, highCard
			}

			hcProjection := extendProjection(projection, e.VectorMatching.MatchingLabels)
			s.optimize(highCard, hcProjection)
			lcProjection := extendProjection(logicalplan.Projection{
				Include: e.VectorMatching.On,
				Labels:  append([]string{SeriesIDColumn}, e.VectorMatching.MatchingLabels...),
			}, e.VectorMatching.Include)
			s.optimize(lowCard, lcProjection)
			stop = true
		case *logicalplan.VectorSelector:
			slices.Sort(projection.Labels)
			projection.Labels = slices.Compact(projection.Labels)
			e.Projection = projection
			projection = logicalplan.Projection{}
		}
	})

	return expr, annotations.Annotations{}
}

func extendProjection(projection logicalplan.Projection, lbls []string) logicalplan.Projection {
	var extendedLabels []string
	if projection.Include {
		extendedLabels = union(projection.Labels, lbls)
	} else {
		extendedLabels = intersect(projection.Labels, lbls)
	}
	return logicalplan.Projection{
		Include: projection.Include,
		Labels:  extendedLabels,
	}
}

// union returns the union of two string slices.
func union(l1 []string, l2 []string) []string {
	m := make(map[string]struct{})
	for _, s := range l1 {
		m[s] = struct{}{}
	}
	for _, s := range l2 {
		m[s] = struct{}{}
	}
	return maps.Keys(m)
}

// intersect returns the intersection of two string slices.
func intersect(l1 []string, l2 []string) []string {
	m := make(map[string]struct{})
	var result []string
	for _, s := range l1 {
		m[s] = struct{}{}
	}
	for _, s := range l2 {
		if _, ok := m[s]; ok {
			result = append(result, s)
		}
	}
	return result
}
