# wal
Write ahead log for Go.

## Features
* transaction
* snapshot
* batch writer
* support key

## Install
```go
go get github.com/aacfactory/wal
```

## Usage
```go
// create
logs, createErr := wal.New[uint64](`file path`, wal.Unit64KeyEncoder(), wal.EnableTransaction(wal.ReadUncommitted))
// write, it will return the index of log
index, writeErr := logs.Write(1, []byte("some content"))
// read 
key, content, state, readErr := logs.Read(index)
// commit
commitErr := logs.Commit(index)
// key
index, content, state, readErr := logs.Key(1)
// close
logs.Close()
```

Batch mode, write multiple at one time, or cancel at one time.
```go
batch := logs.Batch()
indexes := make([]uint64, 0, 1)
for i := 0; i < 3; i++ {
    index, err := batch.Write(uint64(i), []byte(time.Now().Format(time.RFC3339)))
    if err != nil {
        // handle err
        return
    }
    indexes = append(indexes, index)
}
fmt.Println(batch.Flush())
fmt.Println(logs.Commit(indexes...))
```
