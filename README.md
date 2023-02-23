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
logs, createErr := wal.New(`file path`)
// write, it will return the index of log
index, writeErr := logs.Write([]byte("key"), []byte("some content"))
// read 
key, content, state, readErr := logs.Read(index)
// commit
commitErr := logs.Commit(index)
// key
content, state, readErr := logs.Key([]byte("key"))
// close
logs.Close()
```

Batch mode, write multiple at one time, or cancel at one time.
```go
batch := logs.Batch()
indexes := make([]uint64, 0, 1)
for i := 0; i < 3; i++ {
    indexes = append(indexes, batch.Write([]byte("key"), []byte(time.Now().Format(time.RFC3339))))
}
fmt.Println(batch.Flush())
batch.Close()
fmt.Println(logs.Commit(indexes...))
```
