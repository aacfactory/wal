package wal

import (
	"bytes"
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
	"math"
)

func NewEntry(index uint64, key []byte, p []byte) (entry Entry) {
	kLen := uint16(len(key))
	content := bytes.Join([][]byte{key, p}, []byte{})
	code := xxhash.Sum64(content)
	pLen := uint16(len(content))
	size := uint16(math.Ceil(float64(pLen+32) / float64(blockSize-8)))
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
		segment := content[sLow:sHigh]

		binary.BigEndian.PutUint16(block[0:2], i)
		binary.BigEndian.PutUint16(block[2:4], size)
		if i == 0 {
			binary.BigEndian.PutUint32(block[4:8], uint32(sHigh-sLow+24))

			binary.BigEndian.PutUint64(block[8:16], index)
			binary.BigEndian.PutUint16(block[16:18], 1)
			binary.BigEndian.PutUint16(block[18:20], kLen)
			binary.BigEndian.PutUint16(block[20:22], 1)
			binary.BigEndian.PutUint64(block[24:32], code)

			copy(block[32:], segment)
		} else {
			binary.BigEndian.PutUint32(block[4:8], uint32(sHigh-sLow))
			copy(block[8:], segment)
		}
		sLow = sHigh
		sHigh = sLow + blockSize - 8
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

func (entry Entry) State() uint16 {
	return binary.BigEndian.Uint16(entry[16:18])
}

func (entry Entry) Commit() {
	binary.BigEndian.PutUint16(entry[16:18], 2)
	return
}

func (entry Entry) Committed() (ok bool) {
	ok = binary.BigEndian.Uint16(entry[16:18]) == 2
	return
}

func (entry Entry) Discard() {
	binary.BigEndian.PutUint16(entry[16:18], 3)
	return
}

func (entry Entry) Discarded() (ok bool) {
	ok = binary.BigEndian.Uint16(entry[16:18]) == 3
	return
}

func (entry Entry) Finished() (ok bool) {
	ok = binary.BigEndian.Uint16(entry[16:18]) > 1
	return
}

func (entry Entry) Removed() (ok bool) {
	ok = binary.BigEndian.Uint16(entry[20:22]) == 0
	return
}

func (entry Entry) Remove() {
	binary.BigEndian.PutUint16(entry[20:22], 0)
	return
}

func (entry Entry) HashCode() (code uint64) {
	code = binary.BigEndian.Uint64(entry[24:32])
	return
}

func (entry Entry) Key() (key []byte) {
	kLen := binary.BigEndian.Uint16(entry[18:20])
	key = entry[8+24 : 8+24+kLen]
	return
}

func (entry Entry) Data() (key []byte, p []byte) {
	kLen := binary.BigEndian.Uint16(entry[18:20])
	key = make([]byte, 0, kLen)
	p = make([]byte, 0, len(entry))
	n := entry.Blocks()
	for i := uint16(0); i < n; i++ {
		block := Block(entry[i*blockSize : i*blockSize+blockSize])
		data := block.Data()
		if i == 0 {
			key = append(key, data[24:24+kLen]...)
			p = append(p, data[24+kLen:]...)
		} else {
			p = append(p, data...)
		}
	}
	return
}

func (entry Entry) Validate() (ok bool) {
	key, p := entry.Data()
	content := bytes.Join([][]byte{key, p}, []byte{})
	ok = entry.HashCode() == xxhash.Sum64(content)
	return
}

type posEntry[K ordered] struct {
	entry Entry
	key   K
	pos   uint64
}

type posEntries[K ordered] []*posEntry[K]

func (items posEntries[K]) Len() int {
	return len(items)
}

func (items posEntries[K]) Less(i, j int) bool {
	return items[i].pos < items[j].pos
}

func (items posEntries[K]) Swap(i, j int) {
	items[i], items[j] = items[j], items[i]
	return
}
