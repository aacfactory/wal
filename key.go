package wal

import (
	"encoding/binary"
	"unsafe"
)

type ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

type KeyEncoder[K ordered] interface {
	Encode(key K) (p []byte, err error)
	Decode(p []byte) (key K, err error)
}

func Unit64KeyEncoder() KeyEncoder[uint64] {
	return &unit64KeyEncoder{}
}

type unit64KeyEncoder struct{}

func (k *unit64KeyEncoder) Encode(key uint64) (p []byte, err error) {
	p = make([]byte, 8)
	binary.BigEndian.PutUint64(p, key)
	return
}

func (k *unit64KeyEncoder) Decode(p []byte) (key uint64, err error) {
	key = binary.BigEndian.Uint64(p)
	return
}

func Int64KeyEncoder() KeyEncoder[int64] {
	return &int64KeyEncoder{}
}

type int64KeyEncoder struct{}

func (k *int64KeyEncoder) Encode(key int64) (p []byte, err error) {
	p = make([]byte, 8)
	binary.BigEndian.PutUint64(p, uint64(key))
	return
}

func (k *int64KeyEncoder) Decode(p []byte) (key int64, err error) {
	key = int64(binary.BigEndian.Uint64(p))
	return
}

func StringKeyEncoder() KeyEncoder[string] {
	return &stringKeyEncoder{}
}

type stringKeyEncoder struct{}

func (k *stringKeyEncoder) Encode(key string) (p []byte, err error) {
	p = unsafe.Slice(unsafe.StringData(key), len(key))
	return
}

func (k *stringKeyEncoder) Decode(p []byte) (key string, err error) {
	key = unsafe.String(unsafe.SliceData(p), len(p))
	return
}
