package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cespare/xxhash/v2"
)

func NewEntry(sot SOT, index uint64, key []byte, p []byte) (entry Entry) {
	content := bytes.Join([][]byte{key, p}, []byte{})
	code := xxhash.Sum64(content)

	header := newEntryHeader()
	header.active()
	header.setIndex(index)
	header.setKeyLen(uint16(len(key)))
	header.setHashCode(code)

	entry = Entry(NewTEUs(sot, bytes.Join([][]byte{header, content}, []byte{})))
	return
}

func DecodeEntries(sot SOT, p []byte) (entries []Entry) {
	entries = make([]Entry, 0, 1)
	for {
		if len(p) == 0 {
			break
		}
		sotLen := TEUs(p).Len()
		end := sotLen * sot.value()
		entries = append(entries, p[0:end])
		p = p[end:]
	}
	return
}

func newEntryHeader() EntryHeader {
	return make([]byte, 24)
}

type EntryHeader []byte

func (header EntryHeader) setIndex(index uint64) {
	binary.BigEndian.PutUint64(header[0:8], index)
}

func (header EntryHeader) Index() (index uint64) {
	index = binary.BigEndian.Uint64(header[0:8])
	return
}

func (header EntryHeader) State() (state State) {
	state = State(binary.BigEndian.Uint16(header[8:10]))
	return
}

func (header EntryHeader) commit() {
	binary.BigEndian.PutUint16(header[8:10], CommittedState.value())
	return
}

func (header EntryHeader) Committed() (ok bool) {
	ok = header.State() == CommittedState
	return
}

func (header EntryHeader) discard() {
	binary.BigEndian.PutUint16(header[8:10], DiscardedState.value())
	return
}

func (header EntryHeader) Discarded() (ok bool) {
	ok = header.State() == DiscardedState
	return
}

func (header EntryHeader) StateFinished() (ok bool) {
	ok = header.State() > UncommittedState
	return
}

func (header EntryHeader) Removed() (ok bool) {
	ok = binary.BigEndian.Uint16(header[10:12]) == 1
	return
}

func (header EntryHeader) remove() {
	binary.BigEndian.PutUint16(header[10:12], 1)
	return
}

func (header EntryHeader) active() (ok bool) {
	binary.BigEndian.PutUint16(header[10:12], 0)
	return
}

func (header EntryHeader) setKeyLen(n uint16) {
	binary.BigEndian.PutUint16(header[12:14], n)
}

func (header EntryHeader) keyLen() (n uint16) {
	n = binary.BigEndian.Uint16(header[12:14])
	return
}

func (header EntryHeader) setHashCode(code uint64) {
	binary.BigEndian.PutUint64(header[16:24], code)
}

func (header EntryHeader) HashCode() (code uint64) {
	code = binary.BigEndian.Uint64(header[16:24])
	return
}

func (header EntryHeader) String() (s string) {
	s = fmt.Sprintf(
		"{index: %d, state: %d, removed: %v, key_len:%d, code: %d}",
		header.Index(), header.State(), header.Removed(), header.keyLen(), header.HashCode())
	return
}

type Entry []byte

func (entry Entry) Header() (header EntryHeader) {
	header = TEUs(entry).FirstDataCut(0, 24)
	return
}

func (entry Entry) body() (p []byte) {
	p = TEUs(entry).Data()[24:]
	return
}

func (entry Entry) TEUsLen() (n uint16) {
	n = TEUs(entry).Len()
	return
}

func (entry Entry) Commit() {
	entry.Header().commit()
	return
}

func (entry Entry) Discard() {
	entry.Header().discard()
	return
}

func (entry Entry) Remove() {
	entry.Header().remove()
	return
}

func (entry Entry) Key() (key []byte) {
	key = entry.body()[0:entry.Header().keyLen()]
	return
}

func (entry Entry) Data() (p []byte) {
	keyLen := entry.Header().keyLen()
	p = entry.body()[keyLen:]
	return
}

func (entry Entry) Validate() (ok bool) {
	content := bytes.Join([][]byte{entry.Key(), entry.Data()}, []byte{})
	ok = entry.Header().HashCode() == xxhash.Sum64(content)
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
