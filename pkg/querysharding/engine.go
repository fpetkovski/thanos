package querysharding

import (
	"time"

	"github.com/go-kit/log"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
)

type getQueriersFunc func() []querypb.QueryClient

type engine struct {
	logger      log.Logger
	getQueriers getQueriersFunc
	analyzer    *queryAnalyzer
}

func NewEngine(logger log.Logger, queriers getQueriersFunc) v1.QueryEngine {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &engine{
		logger:      logger,
		getQueriers: queriers,
		analyzer:    NewQueryAnalyzer(),
	}
}

func (d *engine) SetQueryLogger(l promql.QueryLogger) {
	d.logger = l
}

func (d *engine) NewInstantQuery(q storage.Queryable, qs string, ts time.Time) (promql.Query, error) {
	return newQuery(d.logger, d.analyzer, d.getQueriers(), qs, ts)
}

func (d *engine) NewRangeQuery(q storage.Queryable, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return newRangeQuery(d.logger, d.analyzer, d.getQueriers(), qs, start, end, interval)
}
