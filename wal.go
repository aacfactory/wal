package wal

import (
	"errors"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"io"
	"sort"
	"sync"
)

var (
	ErrInvalidEntry = errors.New("invalid entry")
)

func New(path string) (wal *WAL, err error) {
	wal, err = NewWithCacheSize(path, 1024)
	return
}

func NewWithCacheSize(path string, cacheSize uint32) (wal *WAL, err error) {
	file, openErr := OpenFile(path)
	if openErr != nil {
		err = errors.Join(errors.New("create wal failed"), openErr)
		return
	}
	if cacheSize == 0 {
		cacheSize = 1024
	}
	wal = &WAL{
		locker:         sync.RWMutex{},
		indexes:        NewIndexTree(),
		cache:          NewLRU(cacheSize),
		file:           file,
		firstIndex:     0,
		nextIndex:      0,
		nextBlockIndex: 0,
		uncommitted:    NewIndexTree(),
		closed:         false,
		snapshotting:   false,
	}
	err = wal.load()
	return
}

type WAL struct {
	locker         sync.RWMutex
	indexes        *IndexTree
	cache          *LRU
	file           *File
	firstIndex     uint64
	nextIndex      uint64
	nextBlockIndex uint64
	uncommitted    *IndexTree
	snapshotting   bool
	closed         bool
}

func (wal *WAL) FirstIndex() (idx uint64, err error) {
	wal.locker.RLock()
	defer wal.locker.RUnlock()
	if wal.nextIndex == 0 {
		err = errors.New("no entry was wrote")
		return
	}
	idx = wal.firstIndex
	return
}

func (wal *WAL) LastIndex() (idx uint64, err error) {
	wal.locker.RLock()
	defer wal.locker.RUnlock()
	if wal.nextIndex == 0 {
		err = errors.New("no entry was wrote")
		return
	}
	idx = wal.nextIndex - 1
	return
}

func (wal *WAL) Read(index uint64) (p []byte, has bool, err error) {
	wal.locker.RLock()
	entry, got, readErr := wal.read(index)
	wal.locker.RUnlock()
	if readErr != nil {
		err = readErr
		return
	}
	if !got {
		return
	}
	p = entry.Data()
	has = true
	return
}

func (wal *WAL) Write(p []byte) (index uint64, err error) {
	wal.locker.Lock()
	if wal.closed {
		err = errors.Join(errors.New("write write failed"), errors.New("wal was closed"))
		wal.locker.Unlock()
		return
	}
	index = wal.nextIndex
	entry := NewEntry(index, p)
	writeErr := wal.file.WriteAt(entry, wal.acquireNextBlockPos())
	if writeErr != nil {
		err = errors.Join(errors.New("wal write failed"), writeErr)
		wal.locker.Unlock()
		return
	}
	wal.mountUncommitted(entry)
	wal.locker.Unlock()
	return
}

func (wal *WAL) acquireNextBlockPos() (pos uint64) {
	pos = wal.nextBlockIndex * blockSize
	return
}

func (wal *WAL) mountUncommitted(entry Entry) {
	index := entry.Index()
	pos := wal.acquireNextBlockPos()
	wal.cache.Add(index, entry)
	wal.indexes.Set(index, pos)
	wal.uncommitted.Set(index, pos)
	wal.nextIndex++
	wal.nextBlockIndex += uint64(entry.Blocks())
	return
}

func (wal *WAL) read(index uint64) (entry Entry, has bool, err error) {
	entry, has = wal.cache.Get(index)
	if has {
		if !entry.Validate() {
			err = ErrInvalidEntry
			return
		}
		return
	}
	pos, hasPos := wal.indexes.Get(index)
	if !hasPos {
		err = errors.Join(errors.New("wal read failed"), fmt.Errorf("%d was not found", index))
		return
	}
	entry, err = wal.readFromFile(pos)
	if err != nil {
		if err == ErrInvalidEntry {
			return
		}
		err = errors.Join(errors.New("wal read failed"), err)
		return
	}
	has = true
	wal.cache.Add(index, entry)
	return
}

func (wal *WAL) readFromFile(pos uint64) (entry Entry, err error) {
	block := NewBlock()
	readErr := wal.file.ReadAt(block, pos)
	if readErr != nil {
		err = errors.Join(errors.New("wal read failed"), readErr)
		return
	}
	if !block.Validate() {
		err = ErrInvalidEntry
		return
	}
	_, span := block.Header()
	if span == 1 {
		entry = Entry(block)
		if !entry.Validate() {
			err = ErrInvalidEntry
			return
		}
		return
	}
	entry = make([]byte, blockSize*span)
	readErr = wal.file.ReadAt(entry, pos)
	if readErr != nil {
		err = errors.Join(errors.New("wal read failed"), readErr)
		return
	}
	if !entry.Validate() {
		err = ErrInvalidEntry
		return
	}
	return
}

func (wal *WAL) Commit(indexes ...uint64) (err error) {
	if indexes == nil || len(indexes) == 0 {
		return
	}
	wal.locker.Lock()
	if wal.uncommitted.Len() == 0 {
		err = errors.Join(errors.New("wal commit log failed"), errors.New("there is no uncommitted"))
		wal.locker.Unlock()
		return
	}
	if len(indexes) == 1 {
		err = wal.commit(indexes[0])
	} else {
		err = wal.commitBatch(indexes)
	}
	wal.locker.Unlock()
	return
}

func (wal *WAL) commit(index uint64) (err error) {
	_, uncommitted := wal.uncommitted.Get(index)
	if !uncommitted {
		err = errors.Join(errors.New("wal commit log failed"), fmt.Errorf("%d was not found", index))
		return
	}
	entry, has, readErr := wal.read(index)
	if readErr != nil {
		err = errors.Join(errors.New("wal commit log failed"), readErr)
		return
	}
	if !has {
		err = errors.Join(errors.New("wal commit log failed"), fmt.Errorf("%d was not found", index))
		return
	}
	if entry.Committed() {
		wal.uncommitted.Remove(index)
		return
	}
	pos, hasPos := wal.indexes.Get(index)
	if !hasPos {
		wal.cache.Remove(index)
		err = errors.Join(errors.New("wal commit log failed"), fmt.Errorf("%d was not found", index))
		return
	}
	entry.Commit()
	writeErr := wal.file.WriteAt(entry, pos)
	if writeErr != nil {
		err = errors.Join(errors.New("wal commit log failed"), fmt.Errorf("commit %d failed", index), writeErr)
		return
	}
	wal.uncommitted.Remove(index)
	return
}

func (wal *WAL) commitBatch(indexes []uint64) (err error) {
	items := posEntries(make([]*posEntry, 0, len(indexes)))
	for _, index := range indexes {
		_, uncommitted := wal.uncommitted.Get(index)
		if !uncommitted {
			continue
		}
		entry, has, readErr := wal.read(index)
		if readErr != nil {
			err = errors.Join(errors.New("wal commit log failed"), readErr)
			return
		}
		if !has {
			err = errors.Join(errors.New("wal commit log failed"), fmt.Errorf("%d was not found", index))
			return
		}
		pos, hasPos := wal.indexes.Get(index)
		if !hasPos {
			wal.cache.Remove(index)
			err = errors.Join(errors.New("wal commit log failed"), fmt.Errorf("%d was not found", index))
			return
		}
		items = append(items, &posEntry{
			entry: entry,
			pos:   pos,
		})
	}
	sort.Sort(items)
	pos := items[0].pos
	segment := make([]byte, 0, blockSize)
	segment = append(segment, items[0].entry...)
	for i := 1; i < len(items); i++ {
		low := items[i-1].pos
		high := items[i].pos
		lowLen := uint64(len(items[i-1].entry))
		if high-low == lowLen {
			segment = append(segment, items[i].entry...)
			continue
		}
		lostPos := low + lowLen
		lostLen := high - lostPos
		lost := make([]byte, lostLen)

		readErr := wal.file.ReadAt(lost, lostPos)
		if readErr != nil {
			err = errors.Join(errors.New("wal commit log failed"), readErr)
			return
		}
		segment = append(segment, lost...)
		segment = append(segment, items[i].entry...)
	}
	for _, item := range items {
		item.entry.Commit()
	}
	updateErr := wal.file.WriteAt(segment, pos)
	if updateErr != nil {
		err = errors.Join(errors.New("wal commit log failed"), updateErr)
		return
	}
	for _, item := range items {
		item.entry.Commit()
		wal.uncommitted.Remove(item.entry.Index())
	}
	return
}

func (wal *WAL) Close() {
	wal.locker.Lock()
	wal.closed = true
	wal.file.Close()
	wal.locker.Unlock()
	return
}

func (wal *WAL) Batch() (batch *Batch) {
	wal.locker.Lock()
	batch = &Batch{
		wal:       wal,
		data:      make([]byte, 0, blockSize),
		nextIndex: wal.nextIndex,
		released:  false,
	}
	return
}

func (wal *WAL) Uncommitted() (n uint64) {
	wal.locker.RLock()
	n = uint64(wal.uncommitted.Len())
	wal.locker.RUnlock()
	return
}

func (wal *WAL) HasCommitted(index uint64) (ok bool) {
	wal.locker.RLock()
	_, got := wal.uncommitted.Get(index)
	ok = !got
	wal.locker.RUnlock()
	return
}

func (wal *WAL) OldestUncommitted() (index uint64, has bool) {
	wal.locker.RLock()
	index, _, has = wal.uncommitted.Min()
	wal.locker.RUnlock()
	return
}

func (wal *WAL) load() (err error) {
	wal.nextBlockIndex = wal.file.Size() / blockSize
	if wal.nextBlockIndex == 0 {
		wal.nextIndex = 0
		wal.firstIndex = 0
		return
	}
	readBlockIndex := uint64(0)
	for readBlockIndex < wal.nextBlockIndex {
		pos := readBlockIndex * blockSize
		entry, readErr := wal.readFromFile(pos)
		if readErr != nil {
			if readErr != ErrInvalidEntry {
				err = errors.Join(errors.New("wal load failed"), readErr)
				return
			}
			continue
		}
		index := entry.Index()
		wal.indexes.Set(index, pos)
		if !entry.Committed() {
			wal.uncommitted.Set(index, pos)
		}
		readBlockIndex += uint64(entry.Blocks())
	}
	minIndex, _, hasMinIndex := wal.indexes.Min()
	if hasMinIndex {
		wal.firstIndex = minIndex
	}
	maxIndex, _, hasMaxIndex := wal.indexes.Max()
	if hasMaxIndex {
		wal.nextIndex = maxIndex + 1
	}
	return
}

func (wal *WAL) CreateSnapshot(sink io.Writer) (err error) {
	wal.locker.Lock()
	if wal.snapshotting {
		err = errors.Join(errors.New("wal create snapshot failed"), errors.New("the last snapshot has not been completed"))
		wal.locker.Unlock()
		return
	}
	wal.snapshotting = true
	wal.locker.Unlock()
	defer wal.closeSnapshotting()
	// get oldest uncommitted entry
	wal.locker.RLock()
	minIndex, _, hasMin := wal.uncommitted.Min()
	wal.locker.RUnlock()
	if !hasMin {
		return
	}

	// write into sink
	reads := 0
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	for i := uint64(0); i < minIndex; i++ {
		entry, has, readErr := wal.read(i)
		if readErr != nil {
			if readErr != ErrInvalidEntry {
				err = errors.Join(errors.New("wal create snapshot failed"), readErr)
				return
			}
		}
		if !has {
			continue
		}
		reads++
		_, _ = buf.Write(entry)
		if reads < 10 {
			continue
		}
		p := buf.Bytes()
		pLen := len(p)
		n := 0
		for n < pLen {
			nn, writeErr := sink.Write(p[n:])
			if writeErr != nil {
				err = errors.Join(errors.New("wal create snapshot failed"), writeErr)
				return
			}
			n += nn
		}
		buf.Reset()
		reads = 0
	}
	return
}

func (wal *WAL) closeSnapshotting() {
	wal.locker.Lock()
	wal.snapshotting = false
	wal.locker.Unlock()
}
