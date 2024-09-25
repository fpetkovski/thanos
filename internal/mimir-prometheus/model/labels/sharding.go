package labels

import (
	"github.com/cespare/xxhash/v2"
)

// StableHash is a labels hashing implementation which is guaranteed to not change over time.
// This function should be used whenever labels hashing backward compatibility must be guaranteed.
func StableHash(ls Labels) uint64 {
	// Use xxhash.Sum64(b) for fast path as it's faster.
	b := make([]byte, 0, 1024)
	ls.Range(func(l Label) {
		b = append(b, l.Name...)
		b = append(b, seps[0])
		b = append(b, l.Value...)
		b = append(b, seps[0])
	})
	return xxhash.Sum64(b)
}
