// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"fmt"
	"strings"
)

func (m *QueryHints) toPromQL(labelMatchers []LabelMatcher) string {
	grouping := m.Grouping.toPromQL()
	matchers := MatchersToString(labelMatchers...)
	queryRange := m.Range.toPromQL()

	query := fmt.Sprintf("%s %s (%s%s)", m.Func.Name, grouping, matchers, queryRange)
	// Remove double spaces if some expressions are missing.
	return strings.Join(strings.Fields(query), " ")
}

func (m *QueryHints) GetGroupingBy() bool {
	if m == nil || m.Grouping == nil {
		return false
	}

	return m.Grouping.By
}

func (m *QueryHints) GetGroupingLabelSet() map[string]struct{} {
	labelSet := make(map[string]struct{})
	if m == nil || m.Grouping == nil {
		return labelSet
	}

	for _, label := range m.Grouping.Labels {
		labelSet[label] = struct{}{}
	}

	return labelSet
}

func (m *Grouping) toPromQL() string {
	if m == nil {
		return ""
	}

	if len(m.Labels) == 0 {
		return ""
	}
	var op string
	if m.By {
		op = "by"
	} else {
		op = "without"
	}

	return fmt.Sprintf("%s (%s)", op, strings.Join(m.Labels, ","))
}

func (m *Grouping) labelSet() map[string]struct{} {
	if m == nil {
		return nil
	}

	labelset := make(map[string]struct{})
	for _, label := range m.Labels {
		labelset[label] = struct{}{}
	}

	return labelset
}

func (m *Range) toPromQL() string {
	if m == nil {
		return ""
	}

	if m.Millis == 0 {
		return ""
	}
	return fmt.Sprintf("[%dms]", m.Millis)
}
