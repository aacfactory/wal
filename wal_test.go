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
	for i := 0; i < 11; i++ {
		p, has, readErr := logs.Read(uint64(i))
		if readErr != nil {
			t.Error(i, "read:", readErr)
			continue
		}
		fmt.Println(i, "read:", has, string(p))
	}
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
