package wal

import (
	"github.com/aacfactory/errors"
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
		err = errors.ServiceError("batch write failed").WithCause(errors.ServiceError("batch was released"))
		return
	}

	if batch.wal.transactionEnabled {
		_, has := batch.wal.uncommittedKeys.Get(key)
		if has {
			err = errors.ServiceError("batch write failed").WithCause(errors.ServiceError("prev key was not Committed or Discarded"))
			batch.release()
			return
		}
	}

	kp, encodeErr := batch.wal.keyEncoder.Encode(key)
	if encodeErr != nil {
		err = encodeErr
		batch.release()
		return
	}
	index = batch.nextIndex
	entry := NewEntry(batch.wal.sot, index, kp, p)
	if !batch.wal.transactionEnabled {
		entry.Commit()
	}
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
		err = errors.ServiceError("wal batch flush failed").WithCause(ErrClosed)
		return
	}
	pos := batch.wal.acquireNextTEUPos()
	writeErr := batch.wal.file.WriteAt(batch.data, pos)
	if writeErr != nil {
		err = errors.ServiceError("flush batch wrote failed").WithCause(writeErr)
		return
	}
	entries := DecodeEntries(batch.wal.sot, batch.data)
	for i, entry := range entries {
		if !batch.wal.transactionEnabled {
			batch.wal.mountWriteCommitted(batch.keys[i], entry, pos)
		} else {
			batch.wal.mountWriteUncommitted(batch.keys[i], entry, pos)
		}
		pos = pos + uint64(entry.TEUsLen()*batch.wal.sot.value())
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
	batch.nextIndex = 0
	batch.wal.locker.Unlock()
}
