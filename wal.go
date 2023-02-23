package wal

import (
	"errors"
	"fmt"
	"github.com/aacfactory/wal/btree"
	"github.com/aacfactory/wal/lru"
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
	ErrNotFound     = errors.New("not found")
)

const (
	UncommittedState = State(iota + 1)
	CommittedState
	DiscardedState
)

type State int

func (state State) String() string {
	switch state {
	case UncommittedState:
		return "uncommitted"
	case DiscardedState:
		return "discarded"
	case CommittedState:
		return "committed"
	default:
		return fmt.Sprintf("Unknown(%d)", state)
	}
}

func New[K ordered](path string, keyEncoder KeyEncoder[K]) (wal *WAL[K], err error) {
	wal, err = NewWithCacheSize[K](path, keyEncoder, 10240)
	return
}

func NewWithCacheSize[K ordered](path string, keyEncoder KeyEncoder[K], cacheSize uint32) (wal *WAL[K], err error) {
	if keyEncoder == nil {
		err = errors.Join(errors.New("create wal failed"), errors.New("key encoder is required"))
		return
	}
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
	wal = &WAL[K]{
		locker:       sync.RWMutex{},
		indexes:      btree.New[uint64, uint64](),
		keys:         btree.New[K, uint64](),
		keyEncoder:   keyEncoder,
		cache:        lru.New[uint64, Entry](cacheSize),
		file:         file,
		nextIndex:    0,
		nextBlockPos: 0,
		uncommitted:  btree.New[uint64, uint64](),
		closed:       false,
		snapshotting: false,
		truncating:   sync.WaitGroup{},
	}
	err = wal.load()
	return
}

type WAL[K ordered] struct {
	locker       sync.RWMutex
	indexes      *btree.BTree[uint64, uint64]
	keys         *btree.BTree[K, uint64]
	keyEncoder   KeyEncoder[K]
	cache        *lru.LRU[uint64, Entry]
	file         *File
	nextIndex    uint64
	nextBlockPos uint64
	uncommitted  *btree.BTree[uint64, uint64]
	snapshotting bool
	truncating   sync.WaitGroup
	closed       bool
}

func (wal *WAL[K]) FirstIndex() (idx uint64, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	defer wal.locker.RUnlock()
	if wal.closed {
		err = ErrClosed
		return
	}
	min, _, has := wal.indexes.Min()
	if !has {
		err = ErrNoFirstIndex
		return
	}
	idx = min
	return
}

func (wal *WAL[K]) LastIndex() (idx uint64, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	defer wal.locker.RUnlock()
	if wal.closed {
		err = ErrClosed
		return
	}
	max, _, has := wal.indexes.Max()
	if !has {
		err = ErrNoLastIndex
		return
	}
	idx = max
	return
}

func (wal *WAL[K]) FirstKey() (k K, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	defer wal.locker.RUnlock()
	if wal.closed {
		err = ErrClosed
		return
	}
	min, _, has := wal.keys.Min()
	if !has {
		err = ErrNoFirstIndex
		return
	}
	k = min
	return
}

func (wal *WAL[K]) LastKey() (k K, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	defer wal.locker.RUnlock()
	if wal.closed {
		err = ErrClosed
		return
	}
	max, _, has := wal.keys.Max()
	if !has {
		err = ErrNoLastIndex
		return
	}
	k = max
	return
}

func (wal *WAL[K]) Len() (n uint64) {
	wal.truncating.Wait()
	wal.locker.RLock()
	n = uint64(wal.indexes.Len())
	wal.locker.RUnlock()
	return
}

func (wal *WAL[K]) Read(index uint64) (key K, p []byte, state State, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	if wal.closed {
		err = ErrClosed
		wal.locker.RUnlock()
		return
	}
	entry, _, got, readErr := wal.read(index)
	wal.locker.RUnlock()
	if readErr != nil {
		err = readErr
		return
	}
	if !got {
		err = ErrNotFound
		return
	}

	k, data := entry.Data()
	key, err = wal.keyEncoder.Decode(k)
	if err != nil {
		return
	}
	p = data
	state = State(entry.State())
	return
}

func (wal *WAL[K]) Key(key K) (index uint64, p []byte, state State, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	if wal.closed {
		err = ErrClosed
		wal.locker.RUnlock()
		return
	}
	entry, _, got, readErr := wal.readByKey(key)
	wal.locker.RUnlock()
	if readErr != nil {
		err = readErr
		return
	}
	if !got {
		err = ErrNotFound
		return
	}
	index = entry.Index()
	_, p = entry.Data()
	state = State(entry.State())
	return
}

func (wal *WAL[K]) Write(key K, p []byte) (index uint64, err error) {
	k, encodeErr := wal.keyEncoder.Encode(key)
	if encodeErr != nil {
		err = encodeErr
		return
	}
	wal.truncating.Wait()
	wal.locker.Lock()
	if wal.closed {
		err = ErrClosed
		wal.locker.Unlock()
		return
	}
	if wal.getUncommittedKey(key) {
		err = errors.Join(errors.New("wal write failed"), errors.New("prev key was not committed or discarded"))
		wal.locker.Unlock()
		return
	}

	index = wal.nextIndex
	entry := NewEntry(index, k, p)
	writeErr := wal.file.WriteAt(entry, wal.acquireNextBlockPos())
	if writeErr != nil {
		err = errors.Join(errors.New("wal write failed"), writeErr)
		wal.locker.Unlock()
		return
	}
	wal.mountUncommitted(key, entry)
	wal.locker.Unlock()
	return
}

func (wal *WAL[K]) RemoveKey(key K) (err error) {
	wal.truncating.Wait()
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = ErrClosed
		return
	}
	index, has := wal.keys.Get(key)
	if !has {
		return
	}
	err = wal.remove(index)
	return
}

func (wal *WAL[K]) Remove(index uint64) (err error) {
	wal.truncating.Wait()
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = ErrClosed
		return
	}
	err = wal.remove(index)
	return
}

func (wal *WAL[K]) remove(idx uint64) (err error) {
	entry, pos, has, readErr := wal.read(idx)
	if readErr != nil {
		err = errors.Join(errors.New("wal remove failed"), readErr)
		return
	}
	if !has {
		return
	}
	key, decodeErr := wal.keyEncoder.Decode(entry.Key())
	if decodeErr != nil {
		err = errors.Join(errors.New("wal remove failed"), decodeErr)
		return
	}

	if entry.Removed() {
		wal.cache.Remove(idx)
		wal.indexes.Remove(idx)
		wal.uncommitted.Remove(idx)
		wal.keys.Remove(key)
		return
	}

	entry.Remove()

	err = wal.file.WriteAt(entry, pos)
	if err != nil {
		err = errors.Join(errors.New("wal remove failed"), err)
		return
	}

	wal.cache.Remove(idx)
	wal.indexes.Remove(idx)
	wal.uncommitted.Remove(idx)
	wal.keys.Remove(key)
	return
}

func (wal *WAL[K]) acquireNextBlockPos() (pos uint64) {
	pos = wal.nextBlockPos * blockSize
	return
}

func (wal *WAL[K]) mountUncommitted(key K, entry Entry) {
	index := entry.Index()
	pos := wal.acquireNextBlockPos()
	wal.cache.Add(index, entry)
	wal.indexes.Set(index, pos)
	wal.uncommitted.Set(index, pos)
	wal.keys.Set(key, index)
	wal.nextIndex++
	wal.nextBlockPos += uint64(entry.Blocks())
	return
}

func (wal *WAL[K]) readByKey(key K) (entry Entry, pos uint64, has bool, err error) {
	index, got := wal.keys.Get(key)
	if !got {
		return
	}
	entry, pos, has, err = wal.read(index)
	return
}

func (wal *WAL[K]) read(index uint64) (entry Entry, pos uint64, has bool, err error) {
	pos, has = wal.indexes.Get(index)
	if !has {
		return
	}
	entry, has = wal.cache.Get(index)
	if has {
		if !entry.Validate() {
			err = ErrInvalidEntry
			return
		}
		return
	}
	entry, err = wal.readFromFile(pos)
	if err != nil {
		switch err {
		case ErrNotFound, ErrInvalidEntry:
			return
		default:
			err = errors.Join(errors.New("wal read failed"), err)
			return
		}
	}
	has = true
	wal.cache.Add(index, entry)
	return
}

func (wal *WAL[K]) readFromFile(pos uint64) (entry Entry, err error) {
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
		if entry.Removed() {
			err = ErrNotFound
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
	if entry.Removed() {
		err = ErrNotFound
		return
	}
	return
}

func (wal *WAL[K]) CommitKey(keys ...K) (err error) {
	wal.truncating.Wait()
	if keys == nil || len(keys) == 0 {
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
	indexes := make([]uint64, 0, len(keys))
	for _, key := range keys {
		index, has := wal.keys.Get(key)
		if !has {
			err = ErrNotFound
			wal.locker.Unlock()
			return
		}
		indexes = append(indexes, index)
	}
	if len(indexes) == 1 {
		err = wal.commitOrDiscard(indexes[0], false)
	} else {
		err = wal.commitOrDiscardBatch(indexes, false)
	}
	if err != nil {
		if err != ErrNotFound {
			err = errors.Join(errors.New("wal commit log failed"), err)
		}
	}
	wal.locker.Unlock()
	return
}

func (wal *WAL[K]) Commit(indexes ...uint64) (err error) {
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
		err = wal.commitOrDiscard(indexes[0], false)
	} else {
		err = wal.commitOrDiscardBatch(indexes, false)
	}
	if err != nil {
		if err != ErrNotFound {
			err = errors.Join(errors.New("wal commit log failed"), err)
		}
	}
	wal.locker.Unlock()
	return
}

func (wal *WAL[K]) DiscardKey(keys ...K) (err error) {
	wal.truncating.Wait()
	if keys == nil || len(keys) == 0 {
		return
	}
	wal.locker.Lock()
	if wal.closed {
		err = ErrClosed
		wal.locker.Unlock()
		return
	}
	if wal.uncommitted.Len() == 0 {
		err = errors.Join(errors.New("wal discard log failed"), errors.New("there is no uncommitted"))
		wal.locker.Unlock()
		return
	}
	indexes := make([]uint64, 0, len(keys))
	for _, key := range keys {
		index, has := wal.keys.Get(key)
		if !has {
			err = ErrNotFound
			wal.locker.Unlock()
			return
		}
		indexes = append(indexes, index)
	}
	if len(indexes) == 1 {
		err = wal.commitOrDiscard(indexes[0], true)
	} else {
		err = wal.commitOrDiscardBatch(indexes, true)
	}
	if err != nil {
		if err != ErrNotFound {
			err = errors.Join(errors.New("wal discard log failed"), err)
		}
	}
	wal.locker.Unlock()
	return
}

func (wal *WAL[K]) Discard(indexes ...uint64) (err error) {
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
		err = errors.Join(errors.New("wal discard log failed"), errors.New("there is no uncommitted"))
		wal.locker.Unlock()
		return
	}
	if len(indexes) == 1 {
		err = wal.commitOrDiscard(indexes[0], true)
	} else {
		err = wal.commitOrDiscardBatch(indexes, true)
	}
	if err != nil {
		if err != ErrNotFound {
			err = errors.Join(errors.New("wal discard log failed"), err)
		}
	}
	wal.locker.Unlock()
	return
}

func (wal *WAL[K]) commitOrDiscard(index uint64, discard bool) (err error) {
	_, uncommitted := wal.uncommitted.Get(index)
	if !uncommitted {
		err = ErrNotFound
		return
	}
	entry, pos, has, readErr := wal.read(index)
	if readErr != nil {
		err = readErr
		return
	}
	if !has {
		err = ErrNotFound
		return
	}
	if entry.Finished() {
		wal.uncommitted.Remove(index)
		return
	}
	if discard {
		entry.Discard()
	} else {
		entry.Commit()
	}
	writeErr := wal.file.WriteAt(entry, pos)
	if writeErr != nil {
		err = writeErr
		return
	}
	wal.uncommitted.Remove(index)
	return
}

func (wal *WAL[K]) commitOrDiscardBatch(indexes []uint64, discard bool) (err error) {
	items := posEntries(make([]*posEntry, 0, len(indexes)))
	for _, index := range indexes {
		_, uncommitted := wal.uncommitted.Get(index)
		if !uncommitted {
			continue
		}
		entry, pos, has, readErr := wal.read(index)
		if readErr != nil {
			err = readErr
			return
		}
		if !has {
			err = ErrNotFound
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
			err = readErr
			return
		}
		segment = append(segment, lost...)
		segment = append(segment, items[i].entry...)
	}
	for _, item := range items {
		if discard {
			item.entry.Discard()
		} else {
			item.entry.Commit()
		}
	}
	updateErr := wal.file.WriteAt(segment, pos)
	if updateErr != nil {
		err = updateErr
		return
	}
	for _, item := range items {
		item.entry.Commit()
		wal.uncommitted.Remove(item.entry.Index())
	}
	return
}

func (wal *WAL[K]) Close() {
	wal.truncating.Wait()
	wal.locker.Lock()
	if !wal.closed {
		wal.closed = true
		wal.file.Close()
	}
	wal.locker.Unlock()
	return
}

func (wal *WAL[K]) Batch() (batch *Batch[K]) {
	wal.truncating.Wait()
	wal.locker.Lock()
	batch = &Batch[K]{
		wal:       wal,
		keys:      make([]K, 0, 1),
		data:      make([]byte, 0, blockSize),
		nextIndex: wal.nextIndex,
		released:  false,
	}
	return
}

func (wal *WAL[K]) UncommittedSize() (n uint64) {
	wal.truncating.Wait()
	wal.locker.RLock()
	n = uint64(wal.uncommitted.Len())
	wal.locker.RUnlock()
	return
}

func (wal *WAL[K]) Uncommitted(index uint64) (ok bool) {
	wal.truncating.Wait()
	wal.locker.RLock()
	ok = wal.getUncommitted(index)
	wal.locker.RUnlock()
	return
}

func (wal *WAL[K]) getUncommitted(index uint64) (ok bool) {
	_, got := wal.uncommitted.Get(index)
	ok = got
	return
}

func (wal *WAL[K]) UncommittedKey(key K) (ok bool) {
	wal.truncating.Wait()
	wal.locker.RLock()
	ok = wal.getUncommittedKey(key)
	wal.locker.RUnlock()
	return
}

func (wal *WAL[K]) getUncommittedKey(key K) (ok bool) {
	index, has := wal.keys.Get(key)
	if !has {
		return
	}
	_, got := wal.uncommitted.Get(index)
	ok = got
	return
}

func (wal *WAL[K]) OldestUncommitted() (index uint64, has bool) {
	wal.truncating.Wait()
	wal.locker.RLock()
	index, _, has = wal.uncommitted.Min()
	wal.locker.RUnlock()
	return
}

func (wal *WAL[K]) load() (err error) {
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
			switch readErr {
			case ErrNotFound, ErrInvalidEntry:
				readBlockIndex += uint64(entry.Blocks())
				break
			default:
				err = errors.Join(errors.New("wal load failed"), readErr)
				return
			}
			continue
		}

		kp := entry.Key()
		key, decodeErr := wal.keyEncoder.Decode(kp)
		if decodeErr != nil {
			err = errors.Join(errors.New("wal load failed"), decodeErr)
			return
		}
		index := entry.Index()
		wal.indexes.Set(index, pos)
		if !entry.Finished() {
			wal.uncommitted.Set(index, pos)
		}
		wal.keys.Set(key, index)
		wal.cache.Add(index, entry)
		readBlockIndex += uint64(entry.Blocks())
	}
	maxIndex, _, hasMaxIndex := wal.indexes.Max()
	if hasMaxIndex {
		wal.nextIndex = maxIndex + 1
	}
	return
}

// CreateSnapshot contains end index
func (wal *WAL[K]) CreateSnapshot(endIndex uint64, sink io.Writer) (err error) {
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
		entry, _, has, readErr := wal.read(i)
		if readErr != nil {
			err = errors.Join(errors.New("wal read failed"), readErr)
			return
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

func (wal *WAL[K]) closeSnapshotting() {
	wal.locker.Lock()
	wal.snapshotting = false
	wal.locker.Unlock()
}

// TruncateFront truncate logs before and contains endIndex
func (wal *WAL[K]) TruncateFront(endIndex uint64) (err error) {
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
		err = ErrNotFound
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
		entry, _, has, readErr := wal.read(i)
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
		entry, _, has, _ := wal.read(i)
		if has {
			kp := entry.Key()
			key, decodeErr := wal.keyEncoder.Decode(kp)
			if decodeErr != nil {
				err = errors.Join(errors.New("wal truncate failed"), decodeErr)
				return
			}
			wal.keys.Remove(key)
		}
		wal.indexes.Remove(i)
		wal.cache.Remove(i)
	}
	return
}

// TruncateBack truncate logs after and contains startIndex
func (wal *WAL[K]) TruncateBack(startIndex uint64) (err error) {
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
		entry, _, has, _ := wal.read(i)
		if has {
			kp := entry.Key()
			key, decodeErr := wal.keyEncoder.Decode(kp)
			if decodeErr != nil {
				err = errors.Join(errors.New("wal truncate failed"), decodeErr)
				return
			}
			wal.keys.Remove(key)
		}
		wal.indexes.Remove(i)
		wal.uncommitted.Remove(i)
		wal.cache.Remove(i)
	}
	return
}
