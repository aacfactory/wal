package wal_test

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/wal"
	"io"
	"testing"
)

func TestNewTEU(t *testing.T) {
	reader := bytes.NewReader([]byte("0123456789"))
	p := make([]byte, 15)
	fmt.Println(io.ReadFull(reader, p))
	fmt.Println(string(p))
}

func TestNewTEUs(t *testing.T) {
	p := make([]byte, 0, 1)
	for i := 0; i < 10; i++ {
		p = append(p, []byte("0123456789")...)
		p = append(p, '|')
	}
	fmt.Println(string(p))
	teus := wal.NewTEUs(wal.SOT64B, p)
	fmt.Println(teus.Len(), len(teus.Data()), teus.SOT(), bytes.Equal(p, teus.Data()), string(teus.Data()))
}
