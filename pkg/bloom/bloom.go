// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package bloom

import (
	"bytes"

	"github.com/bits-and-blooms/bloom"
)

const FilterErrorRate = 0.01

type Filter interface {
	Test(string) bool
	Bytes() []byte
	Cap() uint
}

type filter struct {
	bloom *bloom.BloomFilter
}

func NewFilterFromBytes(bloomBytes []byte) Filter {
	if bloomBytes == nil {
		return NewAlwaysTrueFilter()
	}

	bloomFilter := &bloom.BloomFilter{}
	byteReader := bytes.NewReader(bloomBytes)
	if _, err := bloomFilter.ReadFrom(byteReader); err != nil {
		return NewAlwaysTrueFilter()
	}

	return &filter{bloom: bloomFilter}
}

func NewFilterForStrings(items ...string) Filter {
	bloomFilter := bloom.NewWithEstimates(uint(len(items)), FilterErrorRate)
	for _, label := range items {
		bloomFilter.AddString(label)
	}

	return &filter{bloom: bloomFilter}
}

func NewFilterFromMapKeys(items map[string]struct{}) Filter {
	bloomFilter := bloom.NewWithEstimates(uint(len(items)), FilterErrorRate)
	for label := range items {
		bloomFilter.AddString(label)
	}

	return &filter{bloom: bloomFilter}
}

func (f filter) Bytes() []byte {
	var buf bytes.Buffer
	if _, err := f.bloom.WriteTo(&buf); err != nil {
		return nil
	}
	return buf.Bytes()
}

func (f filter) Test(s string) bool {
	return f.bloom.TestString(s)
}

func (f filter) Cap() uint {
	return f.bloom.Cap()
}

type alwaysTrueFilter struct{}

func NewAlwaysTrueFilter() *alwaysTrueFilter {
	return &alwaysTrueFilter{}
}

func (e alwaysTrueFilter) Test(s string) bool {
	return true
}

func (e alwaysTrueFilter) Bytes() []byte {
	return nil
}

func (e alwaysTrueFilter) Cap() uint {
	return 1
}
