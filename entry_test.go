package wal_test

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/wal"
	"testing"
)

func TestEntry_Data(t *testing.T) {
	p := make([]byte, 0, 1)
	for i := 0; i < 10; i++ {
		p = append(p, []byte("0123456789")...)
		p = append(p, '|')
	}
	fmt.Println(string(p))
	entry := wal.NewEntry(1, []byte("0"), p)
	key, b := entry.Data()
	fmt.Println(entry.Index(), entry.Blocks(), string(key), bytes.Equal(p, b), string(b))

}
