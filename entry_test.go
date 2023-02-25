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
	entry := wal.NewEntry(wal.SOT64B, 1, []byte("abc"), p)
	entry.Commit()
	entry.Remove()
	key := entry.Key()
	data := entry.Data()
	fmt.Println("header:", entry.Header())
	fmt.Println("teus:", entry.TEUsLen())
	fmt.Println("key:", string(key))
	fmt.Println("data:", bytes.Equal(p, data), string(data))

}
