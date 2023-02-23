package btree

const (
	degree   = 128
	maxItems = degree*2 - 1 // max items per node. max children is +1
	minItems = maxItems / 2
)

type ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

type cow struct {
	_ int
}

type treeItem[K ordered, V any] struct {
	key   K
	value V
}

type treeNode[K ordered, V any] struct {
	cow      *cow
	count    int
	items    []treeItem[K, V]
	children *[]*treeNode[K, V]
}

func New[K ordered, V any]() (tree *BTree[K, V]) {
	tree = &BTree[K, V]{
		cow:   new(cow),
		root:  nil,
		count: 0,
		empty: treeItem[K, V]{},
	}
	return
}

type BTree[K ordered, V any] struct {
	cow   *cow
	root  *treeNode[K, V]
	count int
	empty treeItem[K, V]
}

func (tr *BTree[K, V]) Set(key K, value V) {
	item := treeItem[K, V]{key: key, value: value}
	if tr.root == nil {
		tr.root = tr.newNode(true)
		tr.root.items = append([]treeItem[K, V]{}, item)
		tr.root.count = 1
		tr.count = 1
		return
	}
	replaced, split := tr.nodeSet(&tr.root, item)
	if split {
		left := tr.root
		right, median := tr.nodeSplit(left)
		tr.root = tr.newNode(false)
		*tr.root.children = make([]*treeNode[K, V], 0, maxItems+1)
		*tr.root.children = append([]*treeNode[K, V]{}, left, right)
		tr.root.items = append([]treeItem[K, V]{}, median)
		tr.root.updateCount()
		tr.Set(item.key, item.value)
		return
	}
	if replaced {
		return
	}
	tr.count++
	return
}

func (tr *BTree[K, V]) Get(key K) (V, bool) {
	if tr.root == nil {
		return tr.empty.value, false
	}
	n := tr.root
	for {
		i, found := tr.find(n, key)
		if found {
			return n.items[i].value, true
		}
		if n.leaf() {
			return tr.empty.value, false
		}
		n = (*n.children)[i]
	}
}

func (tr *BTree[K, V]) Remove(key K) bool {
	if tr.root == nil {
		return false
	}
	_, deleted := tr.remove(&tr.root, false, key)
	if !deleted {
		return false
	}
	if len(tr.root.items) == 0 && !tr.root.leaf() {
		tr.root = (*tr.root.children)[0]
	}
	tr.count--
	if tr.count == 0 {
		tr.root = nil
	}
	return true
}

func (tr *BTree[K, V]) Max() (K, V, bool) {
	if tr.root == nil {
		return tr.empty.key, tr.empty.value, false
	}
	n := tr.root
	for {
		if n.leaf() {
			item := n.items[len(n.items)-1]
			return item.key, item.value, true
		}
		n = (*n.children)[len(*n.children)-1]
	}
}

func (tr *BTree[K, V]) Min() (K, V, bool) {
	if tr.root == nil {
		return tr.empty.key, tr.empty.value, false
	}
	n := tr.root
	for {
		if n.leaf() {
			item := n.items[0]
			return item.key, item.value, true
		}
		n = (*n.children)[0]
	}
}

func (tr *BTree[K, V]) Len() int {
	return tr.count
}

func (tr *BTree[K, V]) Height() int {
	var height int
	if tr.root != nil {
		n := tr.root
		for {
			height++
			if n.leaf() {
				break
			}
			n = (*n.children)[0]
		}
	}
	return height
}

func (tr *BTree[K, V]) Load(key K, value V) {
	if tr.root == nil {
		tr.Set(key, value)
		return
	}
	n := tr.cowLoad(&tr.root)
	for {
		n.count++ // optimistically update counts
		if n.leaf() {
			if len(n.items) < maxItems {
				if tr.less(n.items[len(n.items)-1].key, key) {
					n.items = append(n.items, treeItem[K, V]{key: key, value: value})
					tr.count++
					return
				}
			}
			break
		}
		n = tr.cowLoad(&(*n.children)[len(*n.children)-1])
	}
	// revert the counts
	n = tr.root
	for {
		n.count--
		if n.leaf() {
			break
		}
		n = (*n.children)[len(*n.children)-1]
	}
	tr.Set(key, value)
	return
}

func (tr *BTree[K, V]) copy(n *treeNode[K, V]) *treeNode[K, V] {
	n2 := new(treeNode[K, V])
	n2.cow = tr.cow
	n2.count = n.count
	n2.items = make([]treeItem[K, V], len(n.items), cap(n.items))
	copy(n2.items, n.items)
	if !n.leaf() {
		n2.children = new([]*treeNode[K, V])
		*n2.children = make([]*treeNode[K, V], len(*n.children), maxItems+1)
		copy(*n2.children, *n.children)
	}
	return n2
}

func (tr *BTree[K, V]) cowLoad(cn **treeNode[K, V]) *treeNode[K, V] {
	if (*cn).cow != tr.cow {
		*cn = tr.copy(*cn)
	}
	return *cn
}

func (tr *BTree[K, V]) less(a, b K) bool {
	return a < b
}

func (tr *BTree[K, V]) newNode(leaf bool) *treeNode[K, V] {
	n := new(treeNode[K, V])
	n.cow = tr.cow
	if !leaf {
		n.children = new([]*treeNode[K, V])
	}
	return n
}

func (n *treeNode[K, V]) leaf() bool {
	return n.children == nil
}

func (tr *BTree[K, V]) find(n *treeNode[K, V], key K) (int, bool) {
	low := 0
	high := len(n.items)
	for low < high {
		mid := (low + high) / 2
		if !tr.less(key, n.items[mid].key) {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low > 0 && !tr.less(n.items[low-1].key, key) {
		return low - 1, true
	}
	return low, false
}

func (tr *BTree[K, V]) nodeSplit(n *treeNode[K, V]) (right *treeNode[K, V], median treeItem[K, V]) {
	i := maxItems / 2
	median = n.items[i]
	left := tr.newNode(n.leaf())
	left.items = make([]treeItem[K, V], len(n.items[:i]), maxItems/2)
	copy(left.items, n.items[:i])
	if !n.leaf() {
		*left.children = make([]*treeNode[K, V],
			len((*n.children)[:i+1]), maxItems+1)
		copy(*left.children, (*n.children)[:i+1])
	}
	left.updateCount()
	right = tr.newNode(n.leaf())
	right.items = make([]treeItem[K, V], len(n.items[i+1:]), maxItems/2)
	copy(right.items, n.items[i+1:])
	if !n.leaf() {
		*right.children = make([]*treeNode[K, V],
			len((*n.children)[i+1:]), maxItems+1)
		copy(*right.children, (*n.children)[i+1:])
	}
	right.updateCount()
	*n = *left
	return right, median
}

func (n *treeNode[K, V]) updateCount() {
	n.count = len(n.items)
	if !n.leaf() {
		for i := 0; i < len(*n.children); i++ {
			n.count += (*n.children)[i].count
		}
	}
}

func (tr *BTree[K, V]) nodeSet(pn **treeNode[K, V], item treeItem[K, V]) (replaced bool, split bool) {
	n := tr.cowLoad(pn)
	i, found := tr.find(n, item.key)
	if found {
		n.items[i].value = item.value
		return true, false
	}
	if n.leaf() {
		if len(n.items) == maxItems {
			return false, true
		}
		n.items = append(n.items, tr.empty)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = item
		n.count++
		return false, false
	}
	replaced, split = tr.nodeSet(&(*n.children)[i], item)
	if split {
		if len(n.items) == maxItems {
			return false, true
		}
		right, median := tr.nodeSplit((*n.children)[i])
		*n.children = append(*n.children, nil)
		copy((*n.children)[i+1:], (*n.children)[i:])
		(*n.children)[i+1] = right
		n.items = append(n.items, tr.empty)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = median
		return tr.nodeSet(&n, item)
	}
	if !replaced {
		n.count++
	}
	return replaced, false
}

func (tr *BTree[K, V]) remove(pn **treeNode[K, V], max bool, key K) (treeItem[K, V], bool) {
	n := tr.cowLoad(pn)
	var i int
	var found bool
	if max {
		i, found = len(n.items)-1, true
	} else {
		i, found = tr.find(n, key)
	}
	if n.leaf() {
		if found {
			prev := n.items[i]
			copy(n.items[i:], n.items[i+1:])
			n.items[len(n.items)-1] = tr.empty
			n.items = n.items[:len(n.items)-1]
			n.count--
			return prev, true
		}
		return tr.empty, false
	}
	var prev treeItem[K, V]
	var deleted bool
	if found {
		if max {
			i++
			prev, deleted = tr.remove(&(*n.children)[i], true, tr.empty.key)
		} else {
			prev = n.items[i]
			maxItem, _ := tr.remove(&(*n.children)[i], true, tr.empty.key)
			deleted = true
			n.items[i] = maxItem
		}
	} else {
		prev, deleted = tr.remove(&(*n.children)[i], max, key)
	}
	if !deleted {
		return tr.empty, false
	}
	n.count--
	if len((*n.children)[i].items) < minItems {
		tr.rebalanced(n, i)
	}
	return prev, true
}

func (tr *BTree[K, V]) rebalanced(n *treeNode[K, V], i int) {
	if i == len(n.items) {
		i--
	}
	left := tr.cowLoad(&(*n.children)[i])
	right := tr.cowLoad(&(*n.children)[i+1])
	if len(left.items)+len(right.items) < maxItems {
		left.items = append(left.items, n.items[i])
		left.items = append(left.items, right.items...)
		if !left.leaf() {
			*left.children = append(*left.children, *right.children...)
		}
		left.count += right.count + 1
		copy(n.items[i:], n.items[i+1:])
		n.items[len(n.items)-1] = tr.empty
		n.items = n.items[:len(n.items)-1]
		copy((*n.children)[i+1:], (*n.children)[i+2:])
		(*n.children)[len(*n.children)-1] = nil
		*n.children = (*n.children)[:len(*n.children)-1]
	} else if len(left.items) > len(right.items) {
		right.items = append(right.items, tr.empty)
		copy(right.items[1:], right.items)
		right.items[0] = n.items[i]
		right.count++
		n.items[i] = left.items[len(left.items)-1]
		left.items[len(left.items)-1] = tr.empty
		left.items = left.items[:len(left.items)-1]
		left.count--

		if !left.leaf() {
			*right.children = append(*right.children, nil)
			copy((*right.children)[1:], *right.children)
			(*right.children)[0] = (*left.children)[len(*left.children)-1]
			(*left.children)[len(*left.children)-1] = nil
			*left.children = (*left.children)[:len(*left.children)-1]
			left.count -= (*right.children)[0].count
			right.count += (*right.children)[0].count
		}
	} else {
		left.items = append(left.items, n.items[i])
		left.count++
		n.items[i] = right.items[0]
		copy(right.items, right.items[1:])
		right.items[len(right.items)-1] = tr.empty
		right.items = right.items[:len(right.items)-1]
		right.count--

		if !left.leaf() {
			*left.children = append(*left.children, (*right.children)[0])
			copy(*right.children, (*right.children)[1:])
			(*right.children)[len(*right.children)-1] = nil
			*right.children = (*right.children)[:len(*right.children)-1]
			left.count += (*left.children)[len(*left.children)-1].count
			right.count -= (*left.children)[len(*left.children)-1].count
		}
	}
}
