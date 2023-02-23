package wal

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

func OpenFile(path string) (file *File, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return
	}
	f, openErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0640)
	if openErr != nil {
		err = openErr
		return
	}
	stat, statErr := f.Stat()
	if statErr != nil {
		err = statErr
		return
	}
	file = &File{
		locker: sync.RWMutex{},
		file:   f,
		size:   uint64(stat.Size()),
	}
	return
}

type File struct {
	locker sync.RWMutex
	file   *os.File
	size   uint64
}

func (f *File) Size() (n uint64) {
	f.locker.RLock()
	n = f.size
	f.locker.RUnlock()
	return
}

func (f *File) ReadAt(p []byte, start uint64) (err error) {
	f.locker.RLock()
	max := len(p)
	n := 0
	off := int64(start)
	for n < max {
		nn, readErr := f.file.ReadAt(p[n:], off)
		if readErr != nil {
			if readErr == io.EOF {
				readErr = io.ErrUnexpectedEOF
			}
			err = readErr
			f.locker.RUnlock()
			return
		}
		n += nn
		off += int64(nn)
	}
	f.locker.RUnlock()
	return
}

func (f *File) WriteAt(p []byte, start uint64) (err error) {
	f.locker.Lock()
	max := len(p)
	n := 0
	off := int64(start)
	for n < max {
		nn, writeErr := f.file.WriteAt(p[n:], off)
		if writeErr != nil {
			err = writeErr
			f.locker.Unlock()
			return
		}
		n += nn
		off += int64(nn)
	}
	err = f.file.Sync()
	if err != nil {
		f.locker.Unlock()
		return
	}
	f.size += uint64(max)
	f.locker.Unlock()
	return
}

func (f *File) Close() {
	f.locker.Lock()
	_ = f.file.Sync()
	_ = f.file.Close()
	f.locker.Unlock()
	return
}
