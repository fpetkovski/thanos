// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package infopb

import (
	"github.com/thanos-io/thanos/pkg/bloom"
)

func NewBloomFilter(filter bloom.Filter) *BloomFilter {
	return &BloomFilter{
		BloomFilterData: filter.Bytes(),
	}
}
