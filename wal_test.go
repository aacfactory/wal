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
	for i := 0; i < 2; i++ {
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
		p, has, readErr := logs.Read(i)
		if readErr != nil {
			t.Error(i, "read:", readErr)
			continue
		}
		fmt.Println(i, "read:", has, string(p))
	}
	fmt.Println(logs.Uncommitted())
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

func TestWAL_CreateSnapshot(t *testing.T) {

}
