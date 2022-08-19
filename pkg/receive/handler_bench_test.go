// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"testing"
)

func BenchmarkHandler(b *testing.B) {
	series := makeSeries()
	wreq := &prompb.WriteRequest{Timeseries: series}

	appendables := []*fakeAppendable{
		{appender: newFakeAppender(nil, nil, nil)},
		{appender: newFakeAppender(nil, nil, nil)},
		{appender: newFakeAppender(nil, nil, nil)},
	}

	handlers, _ := newTestHandlerHashring(appendables, 3)
	tenant := "test"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for idx, handler := range handlers {
			_, err := makeRequest(handler, tenant, wreq)
			if err != nil {
				b.Fatalf("handler %d: unexpectedly failed making HTTP request: %v", idx, err)
			}
		}
	}
}
