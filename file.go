package wal

import (
	"github.com/aacfactory/errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

func OpenFile(path string) (file *File, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		err = errors.ServiceError("open file failed").WithCause(err).WithMeta("file", path)
		return
	}
	f, openErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0640)
	if openErr != nil {
		err = errors.ServiceError("open file failed").WithCause(openErr).WithMeta("file", path)
		return
	}
	stat, statErr := f.Stat()
	if statErr != nil {
		err = errors.ServiceError("open file failed").WithCause(statErr).WithMeta("file", path)
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

func (f *File) Reopen() (err error) {
	f.locker.Lock()
	defer f.locker.Unlock()
	path := f.Path()
	_ = f.file.Sync()
	_ = f.file.Close()
	file, openErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0640)
	if openErr != nil {
		err = errors.ServiceError("reopen file failed").WithCause(openErr).WithMeta("file", path)
		return
	}
	f.file = file
	return
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
			err = errors.ServiceError("read file failed").WithCause(readErr).WithMeta("file", f.file.Name())
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
			err = errors.ServiceError("write file failed").WithCause(writeErr).WithMeta("file", f.file.Name())
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

func (f *File) Truncate(end uint64) (err error) {
	f.locker.Lock()
	err = f.file.Truncate(int64(end))
	if err != nil {
		err = errors.ServiceError("write file failed").WithCause(err).WithMeta("file", f.file.Name())
	}
	f.locker.Unlock()
	return
}

func (f *File) Path() (path string) {
	path = f.file.Name()
	return
}

func (f *File) Close() {
	f.locker.Lock()
	_ = f.file.Sync()
	_ = f.file.Close()
	f.locker.Unlock()
	return
}

func ExistFile(filePath string) (ok bool) {
	_, err := os.Stat(filePath)
	if err == nil {
		ok = true
		return
	}
	if os.IsNotExist(err) {
		return
	}
	ok = true
	return
}
