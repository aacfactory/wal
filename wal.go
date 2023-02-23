package wal

import (
	"errors"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"io"
	"os"
	"sort"
	"sync"
)

var (
	ErrInvalidEntry = errors.New("invalid entry")
	ErrClosed       = errors.New("wal was closed")
	ErrNoFirstIndex = errors.New("wal has no first index")
	ErrNoLastIndex  = errors.New("wal has no last index")
)

func New(path string) (wal *WAL, err error) {
	wal, err = NewWithCacheSize(path, 1024)
	return
}

func NewWithCacheSize(path string, cacheSize uint32) (wal *WAL, err error) {
	file, openErr := OpenFile(path)
	if openErr != nil {
		// exist .truncated
		if !ExistFile(path + ".truncated") {
			err = errors.Join(errors.New("create wal failed"), openErr)
			return
		}
		// open .truncated
		file, openErr = OpenFile(path + ".truncated")
		if openErr != nil {
			err = errors.Join(errors.New("create wal failed"), openErr)
			return
		}
		file.Close()
		// rename
		renameErr := os.Rename(path+".truncated", path)
		if renameErr != nil {
			err = errors.Join(errors.New("create wal failed"), renameErr)
			return
		}
		// reopen
		file, openErr = OpenFile(path)
		if openErr != nil {
			err = errors.Join(errors.New("create wal failed"), openErr)
			return
		}
	}
	if cacheSize == 0 {
		cacheSize = 1024
	}
	wal = &WAL{
		locker:       sync.RWMutex{},
		indexes:      NewIndexTree(),
		cache:        NewLRU(cacheSize),
		file:         file,
		nextIndex:    0,
		nextBlockPos: 0,
		uncommitted:  NewIndexTree(),
		closed:       false,
		snapshotting: false,
		truncating:   sync.WaitGroup{},
	}
	err = wal.load()
	return
}

type WAL struct {
	locker       sync.RWMutex
	indexes      *IndexTree
	cache        *LRU
	file         *File
	nextIndex    uint64
	nextBlockPos uint64
	uncommitted  *IndexTree
	snapshotting bool
	truncating   sync.WaitGroup
	closed       bool
}

func (wal *WAL) FirstIndex() (idx uint64, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	defer wal.locker.RUnlock()
	if wal.closed {
		err = ErrClosed
		return
	}
	has := false
	idx, _, has = wal.indexes.Min()
	if !has {
		err = ErrNoFirstIndex
		return
	}
	return
}

func (wal *WAL) LastIndex() (idx uint64, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	defer wal.locker.RUnlock()
	if wal.closed {
		err = ErrClosed
		return
	}
	has := false
	idx, _, has = wal.indexes.Max()
	if !has {
		err = ErrNoLastIndex
		return
	}
	return
}

func (wal *WAL) Len() (n uint64) {
	wal.truncating.Wait()
	wal.locker.RLock()
	n = uint64(wal.indexes.Len())
	wal.locker.RUnlock()
	return
}

func (wal *WAL) Read(index uint64) (p []byte, has bool, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	if wal.closed {
		err = ErrClosed
		wal.locker.RUnlock()
		return
	}
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
	wal.truncating.Wait()
	wal.locker.Lock()
	if wal.closed {
		err = ErrClosed
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
	pos = wal.nextBlockPos * blockSize
	return
}

func (wal *WAL) mountUncommitted(entry Entry) {
	index := entry.Index()
	pos := wal.acquireNextBlockPos()
	wal.cache.Add(index, entry)
	wal.indexes.Set(index, pos)
	wal.uncommitted.Set(index, pos)
	wal.nextIndex++
	wal.nextBlockPos += uint64(entry.Blocks())
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
			has = true
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
	wal.truncating.Wait()
	if indexes == nil || len(indexes) == 0 {
		return
	}
	wal.locker.Lock()
	if wal.closed {
		err = ErrClosed
		wal.locker.Unlock()
		return
	}
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
	wal.truncating.Wait()
	wal.locker.Lock()
	if !wal.closed {
		wal.closed = true
		wal.file.Close()
	}
	wal.locker.Unlock()
	return
}

func (wal *WAL) Batch() (batch *Batch) {
	wal.truncating.Wait()
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
	wal.truncating.Wait()
	wal.locker.RLock()
	n = uint64(wal.uncommitted.Len())
	wal.locker.RUnlock()
	return
}

func (wal *WAL) HasCommitted(index uint64) (ok bool) {
	wal.truncating.Wait()
	wal.locker.RLock()
	_, got := wal.uncommitted.Get(index)
	ok = !got
	wal.locker.RUnlock()
	return
}

func (wal *WAL) OldestUncommitted() (index uint64, has bool) {
	wal.truncating.Wait()
	wal.locker.RLock()
	index, _, has = wal.uncommitted.Min()
	wal.locker.RUnlock()
	return
}

func (wal *WAL) load() (err error) {
	if wal.file.Size() == 0 {
		wal.nextBlockPos = 0
		wal.nextIndex = 0
		return
	}
	wal.nextBlockPos = wal.file.Size() / blockSize
	readBlockIndex := uint64(0)
	for readBlockIndex < wal.nextBlockPos {
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
	maxIndex, _, hasMaxIndex := wal.indexes.Max()
	if hasMaxIndex {
		wal.nextIndex = maxIndex + 1
	}
	return
}

// CreateSnapshot contains end index
func (wal *WAL) CreateSnapshot(endIndex uint64, sink io.Writer) (err error) {
	wal.truncating.Wait()
	wal.locker.Lock()
	if wal.closed {
		err = ErrClosed
		wal.locker.Unlock()
		return
	}
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
	firstIndex, _, hasFirst := wal.indexes.Min()
	minUncommittedIndex, _, hasUncommittedMin := wal.uncommitted.Min()
	wal.locker.RUnlock()
	if !hasFirst {
		return
	}
	if hasUncommittedMin && minUncommittedIndex <= endIndex {
		err = errors.Join(errors.New("wal create snapshot failed"), errors.New("min uncommitted index is less than end index"))
		return
	}
	if endIndex < firstIndex {
		err = errors.Join(errors.New("wal create snapshot failed"), errors.New("end index is less than first index"))
		return
	}

	// write into sink
	reads := 0
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	for i := firstIndex; i <= endIndex; i++ {
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
	if buf.Len() > 0 {
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
	}
	return
}

func (wal *WAL) closeSnapshotting() {
	wal.locker.Lock()
	wal.snapshotting = false
	wal.locker.Unlock()
}

// TruncateFront truncate logs before and contains endIndex
func (wal *WAL) TruncateFront(endIndex uint64) (err error) {
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = ErrClosed
		wal.locker.Unlock()
		return
	}
	wal.truncating.Add(1)
	defer wal.truncating.Done()
	if wal.snapshotting {
		err = errors.Join(errors.New("wal truncate failed"), errors.New("can not truncate when making a snapshot"))
		return
	}
	_, hasPos := wal.indexes.Get(endIndex)
	if !hasPos {
		err = errors.Join(errors.New("wal truncate failed"), errors.New(fmt.Sprintf("%d was not found", endIndex)))
		return
	}
	minUnCommittedIndex, _, hasMinUnCommitted := wal.uncommitted.Min()
	if hasMinUnCommitted && minUnCommittedIndex <= endIndex {
		err = errors.Join(errors.New("wal truncate failed"), errors.New(fmt.Sprintf("%d is greater and equals than min uncommitted index", endIndex)))
		return
	}
	minIndex, _, hasMinIndex := wal.indexes.Min()
	if hasMinIndex && minIndex > endIndex {
		err = errors.Join(errors.New("wal truncate failed"), errors.New(fmt.Sprintf("%d is less than min index", endIndex)))
		return
	}
	maxIndex, _, hasMaxIndex := wal.indexes.Max()
	if !hasMaxIndex {
		err = errors.Join(errors.New("wal truncate failed"), errors.New("there is no max index"))
		return
	}

	cleanTmpFn := func(tmpFiles ...*os.File) {
		for _, tmpFile := range tmpFiles {
			_ = tmpFile.Close()
			_ = os.Remove(tmpFile.Name())
		}
	}
	tmpFilepath := wal.file.Path() + ".truncating"
	tmpFile, openTmpErr := os.OpenFile(tmpFilepath, os.O_CREATE|os.O_TRUNC|os.O_SYNC|os.O_RDWR, 0640)
	if openTmpErr != nil {
		err = errors.Join(errors.New("wal truncate failed"), openTmpErr)
		return
	}
	tmpFullFilepath := wal.file.Path() + ".truncated"
	tmpFullFile, openFullErr := os.OpenFile(tmpFullFilepath, os.O_CREATE|os.O_TRUNC|os.O_SYNC|os.O_RDWR, 0640)
	if openFullErr != nil {
		cleanTmpFn(tmpFile)
		err = errors.Join(errors.New("wal truncate failed"), openFullErr)
		return
	}

	writes := uint64(0)
	startIndex := endIndex + 1
	for i := startIndex; i <= maxIndex; i++ {
		entry, has, readErr := wal.read(i)
		if readErr != nil && readErr != ErrInvalidEntry {
			cleanTmpFn(tmpFile, tmpFullFile)
			err = errors.Join(errors.New("wal truncate failed"), readErr)
			return
		}
		if !has || readErr == ErrInvalidEntry {
			continue
		}
		max := len(entry)
		n := 0
		for n < max {
			nn, writeErr := tmpFile.Write(entry[n:])
			if writeErr != nil {
				cleanTmpFn(tmpFile, tmpFullFile)
				err = errors.Join(errors.New("wal truncate failed"), writeErr)
				return
			}
			n += nn
		}
		writes += uint64(max)
	}
	syncErr := tmpFile.Sync()
	if syncErr != nil {
		cleanTmpFn(tmpFile, tmpFullFile)
		err = errors.Join(errors.New("wal truncate failed"), syncErr)
		return
	}

	_, seekErr := tmpFile.Seek(0, 0)
	if seekErr != nil {
		cleanTmpFn(tmpFile, tmpFullFile)
		err = errors.Join(errors.New("wal truncate failed"), seekErr)
		return
	}
	copied, cpErr := io.Copy(tmpFullFile, tmpFile)
	if cpErr != nil {
		cleanTmpFn(tmpFile, tmpFullFile)
		err = errors.Join(errors.New("wal truncate failed"), cpErr)
		return
	}

	if uint64(copied) != writes {
		cleanTmpFn(tmpFile, tmpFullFile)
		err = errors.Join(errors.New("wal truncate failed"), errors.New("not copy full"))
		return
	}
	syncFullErr := tmpFullFile.Sync()
	if syncFullErr != nil {
		cleanTmpFn(tmpFile, tmpFullFile)
		err = errors.Join(errors.New("wal truncate failed"), syncFullErr)
		return
	}
	_ = tmpFile.Close()
	_ = os.Remove(tmpFilepath)

	_ = tmpFullFile.Close()

	originFilepath := wal.file.Path()
	wal.file.Close()

	_ = os.Remove(originFilepath)
	renameErr := os.Rename(tmpFullFilepath, originFilepath)
	if renameErr != nil {
		err = errors.Join(errors.New("wal truncate failed"), renameErr)
		return
	}

	wal.file, err = OpenFile(originFilepath)
	if err != nil {
		err = errors.Join(errors.New("wal truncate failed"), renameErr)
		return
	}

	for i := minIndex; i <= endIndex; i++ {
		wal.indexes.Remove(i)
		wal.cache.Remove(i)
	}
	return
}

// TruncateBack truncate logs after and contains startIndex
func (wal *WAL) TruncateBack(startIndex uint64) (err error) {
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = ErrClosed
		wal.locker.Unlock()
		return
	}
	wal.truncating.Add(1)
	defer wal.truncating.Done()
	if wal.snapshotting {
		err = errors.Join(errors.New("wal truncate failed"), errors.New("can not truncate when making a snapshot"))
		return
	}
	pos, hasPos := wal.indexes.Get(startIndex)
	if !hasPos {
		err = errors.Join(errors.New("wal truncate failed"), errors.New(fmt.Sprintf("%d was not found", startIndex)))
		return
	}
	err = wal.file.Truncate(pos)
	if err != nil {
		err = errors.Join(errors.New("wal truncate failed"), err)
		return
	}
	endIndex, _, hasEnd := wal.indexes.Max()
	if !hasEnd {
		return
	}
	for i := startIndex; i <= endIndex; i++ {
		wal.indexes.Remove(i)
		wal.uncommitted.Remove(i)
		wal.cache.Remove(i)
	}
	return
}
