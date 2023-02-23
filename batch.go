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
	if batch.released {
		err = errors.Join(errors.New("batch write failed"), errors.New("batch was released"))
		return
	}

	if batch.wal.getUncommittedKey(key) {
		err = errors.Join(errors.New("batch write failed"), errors.New("prev key was not committed or discarded"))
		batch.release()
		return
	}

	kp, encodeErr := batch.wal.keyEncoder.Encode(key)
	if encodeErr != nil {
		err = encodeErr
		batch.release()
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
	batch.released = true
	batch.keys = batch.keys[:]
	batch.data = batch.data[:]
	batch.wal.locker.Unlock()
}
