package wal_test

import (
	"encoding/binary"
	"fmt"
	"github.com/aacfactory/wal"
	"os"
	"testing"
	"time"
)

type KeyEncoder struct {
}

func (k *KeyEncoder) Encode(key uint64) (p []byte, err error) {
	p = make([]byte, 8)
	binary.BigEndian.PutUint64(p, key)
	return
}

func (k *KeyEncoder) Decode(p []byte) (key uint64, err error) {
	key = binary.BigEndian.Uint64(p)
	return
}

func newWal(t *testing.T) (logs *wal.WAL[uint64]) {
	var err error
	logs, err = wal.New[uint64](`G:\tmp\wal\1.txt`, &KeyEncoder{})
	if err != nil {
		t.Error(err)
		os.Exit(9)
		return
	}
	return
}

func TestWAL_Write(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	for i := 0; i < 10; i++ {
		index, writeErr := logs.Write(uint64(i), []byte(time.Now().Format(time.RFC3339)))
		if writeErr != nil {
			t.Error(i, "write", writeErr)
			return
		}
		fmt.Println(i, "write:", index)
		cmtErr := logs.Commit(index)
		if cmtErr != nil {
			t.Error(i, "commit:", index, cmtErr)
			return
		}
	}
}

func TestWAL_WriteKey(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.Write(uint64(4), []byte("1")))
	fmt.Println(logs.Write(uint64(4), []byte("2")))
	fmt.Println(logs.CommitKey(uint64(4)))
	index, p, state, err := logs.Key(4)
	fmt.Println(index, string(p), state, err)
	fmt.Println(logs.Write(uint64(4), []byte("3")))
	index, p, state, err = logs.Key(4)
	fmt.Println(index, string(p), state, err)
}

func TestWAL_Read(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	lastIndex, lastErr := logs.LastIndex()
	if lastErr != nil {
		t.Error(lastErr)
		return
	}
	fmt.Println(lastIndex)
	for i := uint64(0); i < lastIndex; i++ {
		key, p, state, readErr := logs.Read(i)
		if readErr != nil {
			t.Error(i, "read:", readErr)
			continue
		}
		fmt.Println(i, "read:", key, state, string(p))
	}
	fmt.Println(logs.UncommittedSize())
}

func TestWAL_Batch(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	batch := logs.Batch()
	defer batch.Close()
	indexes := make([]uint64, 0, 1)
	for i := 0; i < 3; i++ {
		index, err := batch.Write(uint64(i), []byte(time.Now().Format(time.RFC3339)))
		if err != nil {
			t.Error(err)
			return
		}
		indexes = append(indexes, index)
	}
	fmt.Println(batch.Flush())
	fmt.Println(logs.Commit(indexes...))
}

func TestWAL_OldestUncommitted(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.UncommittedSize())
	index, has := logs.OldestUncommitted()
	if !has {
		fmt.Println("non")
		return
	}
	fmt.Println("uncommitted", index, logs.Uncommitted(index))
}

func TestWAL_CreateSnapshot(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	err := logs.CreateSnapshot(10, &SnapshotSink{})
	if err != nil {
		t.Error(err)
	}
}

type SnapshotSink struct {
}

func (s *SnapshotSink) Write(p []byte) (n int, err error) {
	entries := wal.DecodeEntries(p)
	for _, entry := range entries {
		key, data := entry.Data()
		fmt.Println("write:", entry.Index(), string(key), string(data))
	}
	n = len(p)
	return
}

func TestWAL_TruncateFront(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.TruncateFront(5))
	fmt.Println(logs.FirstIndex())
}

func TestWAL_TruncateBack(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.TruncateBack(13))
	fmt.Println(logs.FirstIndex())
	fmt.Println(logs.LastIndex())
}

func TestWAL_FirstIndex(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.FirstIndex())
	fmt.Println(logs.LastIndex())
	fmt.Println(logs.Len())
}

func TestWAL_Key(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	index, p, has, err := logs.Key(1)
	fmt.Println(index, has, string(p), err)
}

func TestWAL_LastKey(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.FirstKey())
	fmt.Println(logs.LastKey())
}

func TestWAL_RemoveKey(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.RemoveKey(0))
	fmt.Println("-----")
	fmt.Println(logs.Remove(18))
}
