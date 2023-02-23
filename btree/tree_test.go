package btree_test

import (
	"fmt"
	"github.com/aacfactory/wal/btree"
	"testing"
)

func TestNew(t *testing.T) {
	tr := btree.New[int, int]()
	tr.Set(1, 1)
	fmt.Println(tr.Get(1))
	tr.Set(1, 2)
	fmt.Println(tr.Get(1))
}
