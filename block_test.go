package wal_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestBlock(t *testing.T) {
	reader := bytes.NewReader([]byte("0123456789"))
	p := make([]byte, 15)
	fmt.Println(io.ReadFull(reader, p))
	fmt.Println(string(p))
}
