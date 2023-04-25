package infopb

import (
	"github.com/thanos-io/thanos/pkg/bloom"
)

func NewBloomFilter(filter bloom.Filter) *BloomFilter {
	return &BloomFilter{
		BloomFilterData: filter.Bytes(),
	}
}
