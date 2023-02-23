package wal_test

import (
	"fmt"
	"github.com/aacfactory/wal"
	"math"
	"testing"
)

func TestInt64KeyEncoder_Decode(t *testing.T) {
	encoder := wal.Int64KeyEncoder()
	n := int64(math.MaxInt64)
	n++
	p, encodeErr := encoder.Encode(n)
	if encodeErr != nil {
		t.Error(encodeErr)
		return
	}
	x, decodeErr := encoder.Decode(p)
	fmt.Println(x == n, decodeErr, uint64(x))
}
