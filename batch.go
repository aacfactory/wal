package wal

import (
	"errors"
)

type Batch struct {
	wal       *WAL
	data      []byte
	nextIndex uint64
	released  bool
}

func (batch *Batch) Write(key []byte, p []byte) (index uint64) {
	index = batch.nextIndex
	entry := NewEntry(index, key, p)
	batch.data = append(batch.data, entry...)
	batch.nextIndex++
	return
}

func (batch *Batch) Flush() (err error) {
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
	for _, entry := range entries {
		batch.wal.mountUncommitted(entry)
	}
	return
}

func (batch *Batch) Cancel() {
	batch.release()
	return
}

func (batch *Batch) Close() {
	batch.release()
	return
}

func (batch *Batch) release() {
	if batch.released {
		return
	}
	batch.wal.locker.Unlock()
	batch.released = true
}
