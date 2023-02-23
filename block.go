package wal

import (
	"encoding/binary"
)

const (
	blockSize = 256
)

func NewBlock() Block {
	return make([]byte, blockSize)
}

type Block []byte

func (block Block) Header() (idx uint16, span uint16) {
	idx = binary.BigEndian.Uint16(block[0:2])
	span = binary.BigEndian.Uint16(block[2:4])
	return
}

func (block Block) Data() (p []byte) {
	size := binary.BigEndian.Uint32(block[4:8])
	p = block[8 : 8+size]
	return
}

func (block Block) Validate() (ok bool) {
	ok = binary.BigEndian.Uint16(block[2:4]) > 0
	return
}
