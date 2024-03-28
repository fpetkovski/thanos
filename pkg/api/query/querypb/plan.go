package querypb

import (
	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/logicalplan"
)

func NewJSONEncodedPlan(plan api.RemoteQuery) (*QueryPlan, error) {
	bytes, err := logicalplan.Marshal(plan.(logicalplan.Node))
	if err != nil {
		return nil, err
	}
	return &QueryPlan{
		Encoding: &QueryPlan_Json{Json: bytes},
	}, nil
}
