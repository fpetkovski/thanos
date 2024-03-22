package querypb

import (
	"encoding/json"

	"github.com/prometheus/prometheus/promql/parser"
)

func NewJSONEncodedPlan(plan parser.Expr) (*QueryPlan, error) {
	bytes, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}

	return &QueryPlan{
		Encoding: &QueryPlan_Json{Json: bytes},
	}, nil
}
