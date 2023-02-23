package wal_test

import (
	"fmt"
	"github.com/aacfactory/wal"
	"os"
	"testing"
	"time"
)

func newWal(t *testing.T) (logs *wal.WAL) {
	var err error
	logs, err = wal.New(`G:\tmp\wal\1.txt`)
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
		index, writeErr := logs.Write([]byte(time.Now().Format(time.RFC3339)))
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
		p, state, readErr := logs.Read(i)
		if readErr != nil {
			t.Error(i, "read:", readErr)
			continue
		}
		fmt.Println(i, "read:", state, string(p))
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
		indexes = append(indexes, batch.Write([]byte(time.Now().Format(time.RFC3339))))
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
		fmt.Println("write:", entry.Index(), string(entry.Data()))
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
