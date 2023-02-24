package wal

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/wal/btree"
	"github.com/aacfactory/wal/lru"
	"github.com/valyala/bytebufferpool"
	"io"
	"os"
	"sort"
	"sync"
)

var (
	ErrInvalidEntry         = fmt.Errorf("invalid entry")
	ErrClosed               = fmt.Errorf("wal was closed")
	ErrNoFirstIndex         = fmt.Errorf("wal has no first index")
	ErrNoLastIndex          = fmt.Errorf("wal has no last index")
	ErrNotFound             = fmt.Errorf("not found")
	ErrCommittedOrDiscarded = fmt.Errorf("entry was committed or discarded")
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

const (
	ReadCommitted = TransactionLevel(iota + 1)
	ReadUncommitted
)

type TransactionLevel int

func (level TransactionLevel) String() string {
	switch level {
	case ReadCommitted:
		return "read_committed"
	case ReadUncommitted:
		return "read_uncommitted"
	default:
		return fmt.Sprintf("Unknown(%d)", level)
	}
}

type Option func(options *Options)

func WithCacheSize(size int) Option {
	return func(options *Options) {
		options.CacheSize = size
	}
}

func EnableTransaction(level TransactionLevel) Option {
	return func(options *Options) {
		options.TransactionEnabled = true
		options.TransactionLevel = level
	}
}

type Options struct {
	CacheSize          int
	TransactionEnabled bool
	TransactionLevel   TransactionLevel
}

func New[K ordered](path string, keyEncoder KeyEncoder[K], options ...Option) (wal *WAL[K], err error) {
	if keyEncoder == nil {
		err = errors.ServiceError("create wal failed").WithCause(errors.ServiceError("key encoder is required"))
		return
	}
	opt := &Options{
		CacheSize:          8196,
		TransactionEnabled: false,
		TransactionLevel:   TransactionLevel(0),
	}
	if options != nil && len(options) > 0 {
		for _, option := range options {
			option(opt)
		}
	}
	if opt.CacheSize < 1 {
		opt.CacheSize = 8196
	}
	if opt.TransactionEnabled {
		if opt.TransactionLevel != ReadUncommitted && opt.TransactionLevel != ReadCommitted {
			err = errors.ServiceError("create wal failed").WithCause(errors.ServiceError("invalid transaction level"))
			return
		}
	}
	file, openErr := OpenFile(path)
	if openErr != nil {
		// exist .truncated
		if !ExistFile(path + ".truncated") {
			err = errors.ServiceError("create wal failed").WithCause(openErr)
			return
		}
		// open .truncated
		file, openErr = OpenFile(path + ".truncated")
		if openErr != nil {
			err = errors.ServiceError("create wal failed").WithCause(openErr)
			return
		}
		file.Close()
		// rename
		renameErr := os.Rename(path+".truncated", path)
		if renameErr != nil {
			err = errors.ServiceError("create wal failed").WithCause(renameErr)
			return
		}
		// reopen
		file, openErr = OpenFile(path)
		if openErr != nil {
			err = errors.ServiceError("create wal failed").WithCause(openErr)
			return
		}
	}

	wal = &WAL[K]{
		locker:             sync.RWMutex{},
		transactionEnabled: opt.TransactionEnabled,
		transactionLevel:   opt.TransactionLevel,
		indexes:            btree.New[uint64, uint64](),
		uncommittedIndexes: btree.New[uint64, uint64](),
		keys:               btree.New[K, uint64](),
		uncommittedKeys:    btree.New[K, uint64](),
		keyEncoder:         keyEncoder,
		cache:              lru.New[uint64, Entry](uint32(opt.CacheSize)),
		file:               file,
		nextIndex:          0,
		nextBlockPos:       0,
		closed:             false,
		snapshotting:       false,
		truncating:         sync.WaitGroup{},
	}
	err = wal.load()
	return
}

type WAL[K ordered] struct {
	locker             sync.RWMutex
	transactionEnabled bool
	transactionLevel   TransactionLevel
	indexes            *btree.BTree[uint64, uint64]
	uncommittedIndexes *btree.BTree[uint64, uint64]
	keys               *btree.BTree[K, uint64]
	uncommittedKeys    *btree.BTree[K, uint64]
	keyEncoder         KeyEncoder[K]
	cache              *lru.LRU[uint64, Entry]
	file               *File
	nextIndex          uint64
	nextBlockPos       uint64
	snapshotting       bool
	truncating         sync.WaitGroup
	closed             bool
}

func (wal *WAL[K]) FirstIndex() (idx uint64, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	defer wal.locker.RUnlock()
	if wal.closed {
		err = errors.ServiceError("wal get first index failed").WithCause(ErrClosed)
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
		err = errors.ServiceError("wal get last index failed").WithCause(ErrClosed)
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
		err = errors.ServiceError("wal get first key failed").WithCause(ErrClosed)
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
		err = errors.ServiceError("wal get last key failed").WithCause(ErrClosed)
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
	if wal.transactionEnabled && wal.transactionLevel == ReadUncommitted {
		n = n + uint64(wal.uncommittedIndexes.Len())
	}
	wal.locker.RUnlock()
	return
}

func (wal *WAL[K]) Read(index uint64) (key K, p []byte, state State, err error) {
	wal.truncating.Wait()
	wal.locker.RLock()
	if wal.closed {
		err = errors.ServiceError("wal read failed").WithCause(ErrClosed)
		wal.locker.RUnlock()
		return
	}
	entry, _, got, readErr := wal.readWithTransaction(index)
	wal.locker.RUnlock()
	if readErr != nil {
		err = errors.ServiceError("wal read failed").WithCause(readErr)
		return
	}
	if !got {
		err = errors.ServiceError("wal read failed").WithCause(ErrNotFound)
		return
	}

	k, data := entry.Data()
	key, err = wal.keyEncoder.Decode(k)
	if err != nil {
		err = errors.ServiceError("wal read failed").WithCause(err)
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
		err = errors.ServiceError("wal read failed").WithCause(ErrClosed)
		wal.locker.RUnlock()
		return
	}
	entry, _, got, readErr := wal.readByKey(key)
	wal.locker.RUnlock()
	if readErr != nil {
		err = errors.ServiceError("wal read failed").WithCause(readErr)
		return
	}
	if !got {
		err = errors.ServiceError("wal read failed").WithCause(ErrNotFound)
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
		err = errors.ServiceError("wal write failed").WithCause(encodeErr)
		return
	}
	wal.truncating.Wait()
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = errors.ServiceError("wal write failed").WithCause(ErrClosed)
		return
	}
	if wal.transactionEnabled {
		_, hasUncommitted := wal.uncommittedKeys.Get(key)
		if hasUncommitted {
			err = errors.ServiceError("wal write failed").WithCause(errors.ServiceError("prev key was not committed or discarded"))
			return
		}
	}

	index = wal.nextIndex
	entry := NewEntry(index, k, p)
	if !wal.transactionEnabled {
		entry.Commit()
	}
	pos := wal.acquireNextBlockPos()
	writeErr := wal.file.WriteAt(entry, pos)
	if writeErr != nil {
		err = errors.ServiceError("wal write failed").WithCause(writeErr)
		return
	}
	if !wal.transactionEnabled {
		wal.mountWriteCommitted(key, entry, pos)
	} else {
		wal.mountWriteUncommitted(key, entry, pos)
	}
	wal.incrNextIndexAndBlockPos(entry)
	//wal.cache.Add(index, entry)
	return
}

func (wal *WAL[K]) RemoveKey(key K) (err error) {
	wal.truncating.Wait()
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = errors.ServiceError("wal remove failed").WithCause(ErrClosed)
		return
	}
	index, has := wal.keys.Get(key)
	if !has {
		index, has = wal.uncommittedKeys.Get(key)
		if !has {
			return
		}
	}
	err = wal.remove(index)
	return
}

func (wal *WAL[K]) Remove(index uint64) (err error) {
	wal.truncating.Wait()
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = errors.ServiceError("wal remove failed").WithCause(ErrClosed)
		return
	}
	err = wal.remove(index)
	return
}

func (wal *WAL[K]) remove(index uint64) (err error) {
	pos, has := wal.indexes.Get(index)
	if !has {
		pos, has = wal.uncommittedIndexes.Get(index)
		if !has {
			err = errors.ServiceError("wal remove failed").WithCause(ErrNotFound)
			return
		}
	}
	entry, readErr := wal.read(index)
	if readErr != nil {
		err = errors.ServiceError("wal remove failed").WithCause(readErr)
		return
	}

	key, decodeErr := wal.keyEncoder.Decode(entry.Key())
	if decodeErr != nil {
		err = errors.ServiceError("wal remove failed").WithCause(decodeErr)
		return
	}

	if !entry.Removed() {
		entry.Remove()
		err = wal.file.WriteAt(entry, pos)
		if err != nil {
			err = errors.ServiceError("wal remove failed").WithCause(err)
			return
		}
	}

	wal.cache.Remove(index)
	wal.indexes.Remove(index)
	wal.uncommittedIndexes.Remove(index)
	wal.keys.Remove(key)
	wal.uncommittedKeys.Remove(key)
	return
}

func (wal *WAL[K]) acquireNextBlockPos() (pos uint64) {
	pos = wal.nextBlockPos * blockSize
	return
}

func (wal *WAL[K]) incrNextIndexAndBlockPos(entry Entry) {
	wal.nextIndex++
	wal.nextBlockPos += uint64(entry.Blocks())
	return
}

func (wal *WAL[K]) mountWriteCommitted(key K, entry Entry, pos uint64) {
	index := entry.Index()
	wal.indexes.Set(index, pos)
	wal.keys.Set(key, index)
	wal.cache.Add(index, entry)
}

func (wal *WAL[K]) mountWriteUncommitted(key K, entry Entry, pos uint64) {
	index := entry.Index()
	wal.uncommittedIndexes.Set(index, pos)
	wal.uncommittedKeys.Set(key, index)
	wal.cache.Add(index, entry)
	return
}

func (wal *WAL[K]) readByKey(key K) (entry Entry, pos uint64, has bool, err error) {
	index, got := wal.keys.Get(key)
	if !got {
		if wal.transactionEnabled && wal.transactionLevel == ReadUncommitted {
			index, got = wal.uncommittedKeys.Get(key)
			if !got {
				return
			}
		} else {
			return
		}
	}
	entry, pos, has, err = wal.readWithTransaction(index)
	return
}

func (wal *WAL[K]) readWithTransaction(index uint64) (entry Entry, pos uint64, has bool, err error) {
	pos, has = wal.indexes.Get(index)
	if !has {
		if wal.transactionEnabled && wal.transactionLevel == ReadUncommitted {
			pos, has = wal.uncommittedIndexes.Get(index)
			if !has {
				return
			}
		} else {
			return
		}
	}
	entry, err = wal.readFromDisk(pos)
	if err != nil {
		err = errors.ServiceError("wal read with transaction failed").WithCause(err)
		return
	}
	has = true
	return
}

func (wal *WAL[K]) read(index uint64) (entry Entry, err error) {
	has := false
	entry, has = wal.cache.Get(index)
	if has {
		return
	}
	pos, hasPos := wal.indexes.Get(index)
	if !hasPos {
		pos, hasPos = wal.uncommittedIndexes.Get(index)
		if !hasPos {
			err = errors.ServiceError("wal read failed").WithCause(ErrNotFound)
			return
		}
	}
	entry, err = wal.readFromDisk(pos)
	if err != nil {
		err = errors.ServiceError("wal read failed").WithCause(err)
		return
	}
	wal.cache.Add(index, entry)
	return
}

func (wal *WAL[K]) readFromDisk(pos uint64) (entry Entry, err error) {
	block := NewBlock()
	readErr := wal.file.ReadAt(block, pos)
	if readErr != nil {
		err = errors.ServiceError("wal read disk failed").WithCause(readErr)
		return
	}
	if !block.Validate() {
		err = errors.ServiceError("wal read disk failed").WithCause(ErrInvalidEntry)
		return
	}
	_, span := block.Header()
	if span == 1 {
		entry = Entry(block)
		if !entry.Validate() {
			err = errors.ServiceError("wal read disk failed").WithCause(ErrInvalidEntry)
			return
		}
		if entry.Removed() {
			err = errors.ServiceError("wal read disk failed").WithCause(ErrNotFound)
			return
		}
		return
	}
	entry = make([]byte, blockSize*span)
	readErr = wal.file.ReadAt(entry, pos)
	if readErr != nil {
		err = errors.ServiceError("wal read disk failed").WithCause(readErr)
		return
	}
	if !entry.Validate() {
		wal.cache.Remove(entry.Index())
		err = errors.ServiceError("wal read disk failed").WithCause(ErrInvalidEntry)
		return
	}
	if entry.Removed() {
		wal.cache.Remove(entry.Index())
		err = errors.ServiceError("wal read disk failed").WithCause(ErrNotFound)
		return
	}
	return
}

func (wal *WAL[K]) CommitKey(keys ...K) (err error) {
	if !wal.transactionEnabled {
		err = errors.ServiceError("wal commit failed").WithCause(errors.ServiceError("transaction mode is disabled"))
		return
	}
	if keys == nil || len(keys) == 0 {
		return
	}
	wal.truncating.Wait()
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = errors.ServiceError("wal commit failed").WithCause(ErrClosed)
		return
	}
	if wal.uncommittedKeys.Len() == 0 {
		err = errors.ServiceError("wal commit failed").WithCause(errors.ServiceError("there is no uncommitted"))
		return
	}
	indexes := make([]uint64, 0, len(keys))
	for _, key := range keys {
		index, has := wal.uncommittedKeys.Get(key)
		if !has {
			err = ErrNotFound
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
		if !errors.Map(err).Contains(ErrNotFound) {
			err = errors.ServiceError("wal commit failed").WithCause(err)
		}
	}
	return
}

func (wal *WAL[K]) Commit(indexes ...uint64) (err error) {
	if !wal.transactionEnabled {
		err = errors.ServiceError("wal commit failed").WithCause(errors.ServiceError("transaction mode is disabled"))
		return
	}
	if indexes == nil || len(indexes) == 0 {
		return
	}
	wal.truncating.Wait()
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = errors.ServiceError("wal commit failed").WithCause(ErrClosed)
		return
	}
	if wal.uncommittedIndexes.Len() == 0 {
		err = errors.ServiceError("wal commit failed").WithCause(errors.ServiceError("there is no uncommitted"))
		return
	}
	if len(indexes) == 1 {
		err = wal.commitOrDiscard(indexes[0], false)
	} else {
		err = wal.commitOrDiscardBatch(indexes, false)
	}
	if err != nil {
		if !errors.Map(err).Contains(ErrNotFound) {
			err = errors.ServiceError("wal commit failed").WithCause(err)
		}
	}
	return
}

func (wal *WAL[K]) DiscardKey(keys ...K) (err error) {
	if !wal.transactionEnabled {
		err = errors.ServiceError("wal discard failed").WithCause(errors.ServiceError("transaction mode is disabled"))
		return
	}
	if keys == nil || len(keys) == 0 {
		return
	}
	wal.truncating.Wait()
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = errors.ServiceError("wal discard failed").WithCause(ErrClosed)
		return
	}
	if wal.uncommittedKeys.Len() == 0 {
		err = errors.ServiceError("wal discard failed").WithCause(errors.ServiceError("there is no uncommitted"))
		return
	}
	indexes := make([]uint64, 0, len(keys))
	for _, key := range keys {
		index, has := wal.uncommittedKeys.Get(key)
		if !has {
			err = errors.ServiceError("wal discard failed").WithCause(ErrNotFound)
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
		if !errors.Map(err).Contains(ErrNotFound) {
			err = errors.ServiceError("wal discard failed").WithCause(err)
		}
	}
	return
}

func (wal *WAL[K]) Discard(indexes ...uint64) (err error) {
	if !wal.transactionEnabled {
		err = errors.ServiceError("wal discard failed").WithCause(errors.ServiceError("transaction mode is disabled"))
		return
	}
	if indexes == nil || len(indexes) == 0 {
		return
	}
	wal.truncating.Wait()
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = errors.ServiceError("wal discard failed").WithCause(errors.ServiceError("there is no uncommitted"))
		return
	}
	if wal.uncommittedIndexes.Len() == 0 {
		err = errors.ServiceError("wal discard failed").WithCause(errors.ServiceError("there is no uncommitted"))
		return
	}
	if len(indexes) == 1 {
		err = wal.commitOrDiscard(indexes[0], true)
	} else {
		err = wal.commitOrDiscardBatch(indexes, true)
	}
	if err != nil {
		if !errors.Map(err).Contains(ErrNotFound) {
			err = errors.ServiceError("wal discard failed").WithCause(err)
		}
	}
	return
}

func (wal *WAL[K]) commitOrDiscard(index uint64, discard bool) (err error) {
	pos, uncommitted := wal.uncommittedIndexes.Get(index)
	if !uncommitted {
		err = ErrCommittedOrDiscarded
		return
	}
	entry, readErr := wal.read(index)
	if readErr != nil {
		err = readErr
		return
	}
	key, decodeKeyErr := wal.keyEncoder.Decode(entry.Key())
	if decodeKeyErr != nil {
		err = decodeKeyErr
		return
	}
	if entry.Finished() {
		wal.uncommittedIndexes.Remove(index)
		wal.uncommittedKeys.Remove(key)
		wal.indexes.Set(index, pos)
		wal.keys.Set(key, index)
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
	wal.cache.Add(index, entry)
	wal.uncommittedIndexes.Remove(index)
	wal.uncommittedKeys.Remove(key)
	wal.indexes.Set(index, pos)
	wal.keys.Set(key, index)
	return
}

func (wal *WAL[K]) commitOrDiscardBatch(indexes []uint64, discard bool) (err error) {
	items := posEntries[K](make([]*posEntry[K], 0, len(indexes)))
	for _, index := range indexes {
		pos, uncommitted := wal.uncommittedIndexes.Get(index)
		if !uncommitted {
			continue
		}
		entry, readErr := wal.read(index)
		if readErr != nil {
			err = readErr
			return
		}
		key, decodeErr := wal.keyEncoder.Decode(entry.Key())
		if decodeErr != nil {
			err = decodeErr
			return
		}
		items = append(items, &posEntry[K]{
			entry: entry,
			key:   key,
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
		index := item.entry.Index()
		key := item.key
		wal.cache.Add(index, item.entry)
		wal.uncommittedIndexes.Remove(index)
		wal.uncommittedKeys.Remove(key)
		wal.indexes.Set(index, pos)
		wal.keys.Set(key, index)
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
	n = uint64(wal.uncommittedIndexes.Len())
	wal.locker.RUnlock()
	return
}

func (wal *WAL[K]) Uncommitted(index uint64) (ok bool) {
	wal.truncating.Wait()
	wal.locker.RLock()
	_, ok = wal.uncommittedIndexes.Get(index)
	wal.locker.RUnlock()
	return
}

func (wal *WAL[K]) UncommittedKey(key K) (ok bool) {
	wal.truncating.Wait()
	wal.locker.RLock()
	_, ok = wal.uncommittedKeys.Get(key)
	wal.locker.RUnlock()
	return
}

func (wal *WAL[K]) OldestUncommitted() (index uint64, has bool) {
	wal.truncating.Wait()
	wal.locker.RLock()
	index, _, has = wal.uncommittedIndexes.Min()
	wal.locker.RUnlock()
	return
}

func (wal *WAL[K]) OldestUncommittedKey() (key K, has bool) {
	wal.truncating.Wait()
	wal.locker.RLock()
	key, _, has = wal.uncommittedKeys.Min()
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
	maxIndex := uint64(0)
	for readBlockIndex < wal.nextBlockPos {
		pos := readBlockIndex * blockSize
		entry, readErr := wal.readFromDisk(pos)
		if readErr != nil {
			switch readErr {
			case ErrNotFound, ErrInvalidEntry:
				readBlockIndex += uint64(entry.Blocks())
				break
			default:
				err = errors.ServiceError("wal load failed").WithCause(readErr)
				return
			}
			continue
		}

		kp := entry.Key()
		key, decodeErr := wal.keyEncoder.Decode(kp)
		if decodeErr != nil {
			err = errors.ServiceError("wal load failed").WithCause(decodeErr)
			return
		}

		index := entry.Index()
		if entry.Finished() {
			wal.indexes.Set(index, pos)
			wal.keys.Set(key, index)
		} else {
			wal.uncommittedIndexes.Set(index, pos)
			wal.uncommittedKeys.Set(key, index)
		}
		wal.cache.Add(index, entry)
		readBlockIndex += uint64(entry.Blocks())
		if index > maxIndex {
			maxIndex = index
		}
	}
	wal.nextIndex = maxIndex + 1
	return
}

func (wal *WAL[K]) reload() (err error) {
	wal.indexes = btree.New[uint64, uint64]()
	wal.uncommittedIndexes = btree.New[uint64, uint64]()
	wal.keys = btree.New[K, uint64]()
	wal.uncommittedKeys = btree.New[K, uint64]()
	wal.cache.Purge()
	wal.nextIndex = 0
	wal.nextBlockPos = 0
	reopenErr := wal.file.Reopen()
	if reopenErr != nil {
		err = errors.ServiceError("wal reload failed").WithCause(reopenErr)
		return
	}
	err = wal.load()
	return
}

// CreateSnapshot contains end index
func (wal *WAL[K]) CreateSnapshot(endIndex uint64, sink io.Writer) (err error) {
	wal.truncating.Wait()
	wal.locker.Lock()
	if wal.closed {
		err = errors.ServiceError("wal create snapshot failed").WithCause(ErrClosed)
		wal.locker.Unlock()
		return
	}
	if wal.snapshotting {
		err = errors.ServiceError("wal create snapshot failed").WithCause(errors.ServiceError("the last snapshot has not been completed"))
		wal.locker.Unlock()
		return
	}
	wal.snapshotting = true
	wal.locker.Unlock()
	defer wal.closeSnapshotting()
	// get oldest uncommitted entry
	wal.locker.RLock()
	firstIndex, _, hasFirst := wal.indexes.Min()
	minUncommittedIndex, _, hasUncommittedMin := wal.uncommittedIndexes.Min()
	wal.locker.RUnlock()
	if !hasFirst {
		return
	}
	if hasUncommittedMin && minUncommittedIndex <= endIndex {
		err = errors.ServiceError("wal create snapshot failed").WithCause(errors.ServiceError("min uncommitted index is less than end index"))
		return
	}
	if endIndex < firstIndex {
		err = errors.ServiceError("wal create snapshot failed").WithCause(errors.ServiceError("end index is less than first index"))
		return
	}

	// write into sink
	reads := 0
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	for i := firstIndex; i <= endIndex; i++ {
		entry, readErr := wal.read(i)
		if readErr != nil {
			if errors.Map(readErr).Contains(ErrNotFound) || errors.Map(readErr).Contains(ErrInvalidEntry) {
				continue
			}
			err = errors.ServiceError("wal create snapshot failed").WithCause(readErr)
			return
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
				err = errors.ServiceError("wal create snapshot failed").WithCause(writeErr)
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
				err = errors.ServiceError("wal create snapshot failed").WithCause(writeErr)
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
		err = errors.ServiceError("wal truncate failed").WithCause(ErrClosed)
		wal.locker.Unlock()
		return
	}
	wal.truncating.Add(1)
	defer wal.truncating.Done()
	if wal.snapshotting {
		err = errors.ServiceError("wal truncate failed").WithCause(errors.ServiceError("can not truncate when making a snapshot"))
		return
	}
	_, has := wal.indexes.Get(endIndex)
	if !has {
		err = ErrNotFound
		return
	}
	minUnCommittedIndex, _, hasMinUnCommitted := wal.uncommittedIndexes.Min()
	if hasMinUnCommitted && minUnCommittedIndex <= endIndex {
		err = errors.ServiceError("wal truncate failed").WithCause(errors.ServiceError(fmt.Sprintf("%d is greater and equals than min uncommitted index", endIndex)))
		return
	}
	minIndex, _, hasMinIndex := wal.indexes.Min()
	if hasMinIndex && minIndex > endIndex {
		err = errors.ServiceError("wal truncate failed").WithCause(errors.ServiceError(fmt.Sprintf("%d is less than min index", endIndex)))
		return
	}
	maxIndex, _, hasMaxIndex := wal.indexes.Max()
	if !hasMaxIndex {
		err = errors.ServiceError("wal truncate failed").WithCause(errors.ServiceError("there is no max index"))
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
		err = errors.ServiceError("wal truncate failed").WithCause(openTmpErr)
		return
	}
	tmpFullFilepath := wal.file.Path() + ".truncated"
	tmpFullFile, openFullErr := os.OpenFile(tmpFullFilepath, os.O_CREATE|os.O_TRUNC|os.O_SYNC|os.O_RDWR, 0640)
	if openFullErr != nil {
		cleanTmpFn(tmpFile)
		err = errors.ServiceError("wal truncate failed").WithCause(openFullErr)
		return
	}

	writes := uint64(0)
	startIndex := endIndex + 1
	for i := startIndex; i <= maxIndex; i++ {
		entry, readErr := wal.read(i)
		if readErr != nil {
			if errors.Map(readErr).Contains(ErrNotFound) || errors.Map(readErr).Contains(ErrInvalidEntry) {
				continue
			}
			cleanTmpFn(tmpFile, tmpFullFile)
			err = errors.ServiceError("wal truncate failed").WithCause(readErr)
			return
		}
		max := len(entry)
		n := 0
		for n < max {
			nn, writeErr := tmpFile.Write(entry[n:])
			if writeErr != nil {
				cleanTmpFn(tmpFile, tmpFullFile)
				err = errors.ServiceError("wal truncate failed").WithCause(writeErr)
				return
			}
			n += nn
		}
		writes += uint64(max)
	}
	syncErr := tmpFile.Sync()
	if syncErr != nil {
		cleanTmpFn(tmpFile, tmpFullFile)
		err = errors.ServiceError("wal truncate failed").WithCause(syncErr)
		return
	}

	_, seekErr := tmpFile.Seek(0, 0)
	if seekErr != nil {
		cleanTmpFn(tmpFile, tmpFullFile)
		err = errors.ServiceError("wal truncate failed").WithCause(seekErr)
		return
	}
	copied, cpErr := io.Copy(tmpFullFile, tmpFile)
	if cpErr != nil {
		cleanTmpFn(tmpFile, tmpFullFile)
		err = errors.ServiceError("wal truncate failed").WithCause(cpErr)
		return
	}

	if uint64(copied) != writes {
		cleanTmpFn(tmpFile, tmpFullFile)
		err = errors.ServiceError("wal truncate failed").WithCause(errors.ServiceError("not copy full"))
		return
	}
	syncFullErr := tmpFullFile.Sync()
	if syncFullErr != nil {
		cleanTmpFn(tmpFile, tmpFullFile)
		err = errors.ServiceError("wal truncate failed").WithCause(syncFullErr)
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
		err = errors.ServiceError("wal truncate failed").WithCause(renameErr)
		return
	}

	wal.file, err = OpenFile(originFilepath)
	if err != nil {
		err = errors.ServiceError("wal truncate failed").WithCause(err)
		return
	}

	err = wal.reload()
	if err != nil {
		err = errors.ServiceError("wal truncate failed").WithCause(err)
		return
	}
	return
}

// TruncateBack truncate logs after and contains startIndex
func (wal *WAL[K]) TruncateBack(startIndex uint64) (err error) {
	wal.locker.Lock()
	defer wal.locker.Unlock()
	if wal.closed {
		err = errors.ServiceError("wal truncate failed").WithCause(ErrClosed)
		wal.locker.Unlock()
		return
	}
	wal.truncating.Add(1)
	defer wal.truncating.Done()
	if wal.snapshotting {
		err = errors.ServiceError("wal truncate failed").WithCause(errors.ServiceError("can not truncate when making a snapshot"))
		return
	}
	pos, hasPos := wal.indexes.Get(startIndex)
	if !hasPos {
		err = errors.ServiceError("wal truncate failed").WithCause(errors.ServiceError(fmt.Sprintf("%d was not found", startIndex)))
		return
	}
	err = wal.file.Truncate(pos)
	if err != nil {
		err = errors.ServiceError("wal truncate failed").WithCause(err)
		return
	}

	err = wal.reload()
	if err != nil {
		err = errors.ServiceError("wal truncate failed").WithCause(err)
		return
	}
	return
}
