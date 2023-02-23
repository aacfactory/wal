package wal

import (
	"errors"
)

type Batch[K ordered] struct {
	wal       *WAL[K]
	keys      []K
	data      []byte
	nextIndex uint64
	released  bool
}

func (batch *Batch[K]) Write(key K, p []byte) (index uint64, err error) {
	kp, encodeErr := batch.wal.keyEncoder.Encode(key)
	if encodeErr != nil {
		err = encodeErr
		return
	}
	index = batch.nextIndex
	entry := NewEntry(index, kp, p)
	batch.keys = append(batch.keys, key)
	batch.data = append(batch.data, entry...)
	batch.nextIndex++
	return
}

func (batch *Batch[K]) Flush() (err error) {
	defer batch.release()
	if len(batch.data) == 0 || batch.released {
		return
	}
	if batch.wal.closed {
		err = ErrClosed
		return
	}
	writeErr := batch.wal.file.WriteAt(batch.data, batch.wal.acquireNextBlockPos())
	if writeErr != nil {
		err = errors.Join(errors.New("flush batch wrote failed"), writeErr)
		return
	}
	entries := DecodeEntries(batch.data)
	for i, entry := range entries {
		batch.wal.mountUncommitted(batch.keys[i], entry)
	}
	return
}

func (batch *Batch[K]) Cancel() {
	batch.release()
	return
}

func (batch *Batch[K]) Close() {
	batch.release()
	return
}

func (batch *Batch[K]) release() {
	if batch.released {
		return
	}
	batch.wal.locker.Unlock()
	batch.released = true
}
