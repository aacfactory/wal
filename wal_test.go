package wal_test

import (
	"fmt"
	"github.com/aacfactory/wal"
	"os"
	"testing"
)

func TestWal_NoTransaction(t *testing.T) {
	logs, err := wal.New[uint64](`G:\tmp\wal\1.txt`, wal.Unit64KeyEncoder(), wal.UseSOT(wal.SOT64B))
	if err != nil {
		t.Log(fmt.Sprintf("%+v", err))
		return
	}
	defer logs.Close()
	// write
	index, writeErr := logs.Write(1, []byte("0"))
	if writeErr != nil {
		t.Error(writeErr)
		return
	}
	// batch write
	batch := logs.Batch()
	defer batch.Close()
	for i := 1; i < 3; i++ {
		_, bwErr := batch.Write(uint64(i), []byte(fmt.Sprintf("%d", i)))
		if bwErr != nil {
			t.Error(bwErr)
			return
		}
	}
	flushErr := batch.Flush()
	if flushErr != nil {
		t.Error(flushErr)
		return
	}
	// read
	key, p, state, readErr := logs.Read(index)
	if readErr != nil {
		t.Error(readErr)
		return
	}
	fmt.Println(index, key, state, string(p))
	index, p, state, readErr = logs.Key(key)
	if readErr != nil {
		t.Error(readErr)
		return
	}
	fmt.Println(index, key, state, string(p))
}

func TestWAL_Transaction(t *testing.T) {
	logs, err := wal.New[uint64](`G:\tmp\wal\1.txt`, wal.Unit64KeyEncoder(), wal.EnableTransaction(wal.ReadUncommitted))
	if err != nil {
		t.Error(err)
		return
	}
	defer logs.Close()
	// write
	index, writeErr := logs.Write(0, []byte("0"))
	if writeErr != nil {
		t.Error(writeErr)
		return
	}
	// batch write
	batch := logs.Batch()
	defer batch.Close()
	for i := 1; i < 10; i++ {
		_, bwErr := batch.Write(uint64(i), []byte(fmt.Sprintf("%d", i)))
		if bwErr != nil {
			t.Error(bwErr)
			return
		}
	}
	flushErr := batch.Flush()
	if flushErr != nil {
		t.Error(flushErr)
		return
	}
	// commit
	for i := 0; i < 10; i++ {
		cmtErr := logs.CommitKey(uint64(i))
		if cmtErr != nil {
			t.Error(cmtErr)
			return
		}
	}
	// read
	key, p, state, readErr := logs.Read(index)
	if readErr != nil {
		t.Error(readErr)
		return
	}
	fmt.Println(index, key, state, string(p))
	index, p, state, readErr = logs.Key(key)
	if readErr != nil {
		t.Error(readErr)
		return
	}
	fmt.Println(index, key, state, string(p))
}

func newWal(t *testing.T) (logs *wal.WAL[uint64]) {
	var err error
	logs, err = wal.New[uint64](`G:\tmp\wal\1.txt`, wal.Unit64KeyEncoder(), wal.EnableTransaction(wal.ReadUncommitted))
	if err != nil {
		t.Log(fmt.Sprintf("%+v", err))
		os.Exit(9)
		return
	}
	return
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
	err := logs.CreateSnapshot(10, &SnapshotSink{
		sot: wal.SOT1K,
	})
	if err != nil {
		t.Error(err)
	}
}

type SnapshotSink struct {
	sot wal.SOT
}

func (s *SnapshotSink) Write(p []byte) (n int, err error) {
	entries := wal.DecodeEntries(s.sot, p)
	for _, entry := range entries {
		key := entry.Key()
		data := entry.Data()
		k, _ := wal.Unit64KeyEncoder().Decode(key)
		fmt.Println("write:", entry.Header().Index(), k, string(data))
	}

	n = len(p)
	return
}

func TestWAL_TruncateFront(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.TruncateFront(3))
	fmt.Println(logs.FirstIndex())
}

func TestWAL_TruncateBack(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.TruncateBack(2))
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

func TestWAL_LastKey(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.FirstKey())
	fmt.Println(logs.LastKey())
}

func TestWAL_RemoveKey(t *testing.T) {
	logs := newWal(t)
	defer logs.Close()
	fmt.Println(logs.RemoveKey(4))
	fmt.Println("-----")
	fmt.Println(logs.Remove(18))
}
