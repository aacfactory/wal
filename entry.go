package wal

import (
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
	"math"
)

func NewEntry(index uint64, p []byte) (entry Entry) {
	code := xxhash.Sum64(p)
	pLen := uint16(len(p))
	size := uint16(math.Ceil(float64(pLen+8) / float64(blockSize-8)))
	entry = make([]byte, blockSize*size)

	sLow := uint16(0)
	sHigh := uint16(blockSize - 32)
	for i := uint16(0); i < size; i++ {
		bLow := i * blockSize
		bHigh := bLow + blockSize
		block := entry[bLow:bHigh]
		if size == 1 || sHigh > pLen {
			sHigh = pLen
		}
		segment := p[sLow:sHigh]
		binary.BigEndian.PutUint16(block[0:2], i)
		binary.BigEndian.PutUint16(block[2:4], size)
		if i == 0 {
			binary.BigEndian.PutUint32(block[4:8], uint32(sHigh-sLow+24))

			binary.BigEndian.PutUint64(block[8:16], index)
			binary.BigEndian.PutUint64(block[16:24], 0)
			binary.BigEndian.PutUint64(block[24:32], code)
			copy(block[32:], segment)
		} else {
			binary.BigEndian.PutUint32(block[4:8], uint32(sHigh-sLow))
			copy(block[8:], segment)
		}
		sLow = sHigh
		sHigh = sLow + blockSize - 24
	}
	return
}

func DecodeEntries(p []byte) (entries []Entry) {
	entries = make([]Entry, 0, 1)
	for {
		if len(p) == 0 {
			break
		}
		span := binary.BigEndian.Uint16(p[2:4])
		end := span * blockSize
		entries = append(entries, p[0:end])
		p = p[end:]
	}
	return
}

type Entry []byte

func (entry Entry) Blocks() (n uint16) {
	_, n = Block(entry[0:blockSize]).Header()
	return
}

func (entry Entry) Index() (index uint64) {
	index = binary.BigEndian.Uint64(entry[8:16])
	return
}

func (entry Entry) Commit() {
	binary.BigEndian.PutUint64(entry[16:24], 1)
	return
}

func (entry Entry) Committed() (ok bool) {
	ok = binary.BigEndian.Uint64(entry[16:24]) == 1
	return
}

func (entry Entry) HashCode() (code uint64) {
	code = binary.BigEndian.Uint64(entry[24:32])
	return
}

func (entry Entry) Data() (p []byte) {
	p = make([]byte, 0, len(entry))
	n := entry.Blocks()
	for i := uint16(0); i < n; i++ {
		block := Block(entry[i*blockSize : i*blockSize+blockSize])
		data := block.Data()
		if i == 0 {
			p = append(p, data[24:]...)
		} else {
			p = append(p, data...)
		}
	}
	return
}

func (entry Entry) Validate() (ok bool) {
	ok = entry.HashCode() == xxhash.Sum64(entry.Data())
	return
}

type posEntry struct {
	entry Entry
	pos   uint64
}

type posEntries []*posEntry

func (items posEntries) Len() int {
	return len(items)
}

func (items posEntries) Less(i, j int) bool {
	return items[i].pos < items[j].pos
}

func (items posEntries) Swap(i, j int) {
	items[i], items[j] = items[j], items[i]
	return
}
