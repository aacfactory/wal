package lru

import (
	"container/list"
)

type LRU[K any, V any] struct {
	size      int64
	evictList *list.List
	items     map[interface{}]*list.Element
}

type lruItem[K any, V any] struct {
	key   K
	value V
}

func New[K any, V any](size uint32) (lru *LRU[K, V]) {
	lru = &LRU[K, V]{
		size:      int64(size),
		evictList: list.New(),
		items:     make(map[interface{}]*list.Element),
	}
	return
}

func (c *LRU[K, V]) Purge() {
	for k := range c.items {
		delete(c.items, k)
	}
	c.evictList.Init()
}

func (c *LRU[K, V]) Add(key K, value V) (evicted bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*lruItem[K, V]).value = value
		return
	}

	ent := &lruItem[K, V]{key, value}
	c.items[key] = c.evictList.PushFront(ent)

	evicted = int64(c.evictList.Len()) > c.size
	if evicted {
		c.removeOldest()
	}
	return
}

func (c *LRU[K, V]) Get(key K) (value V, ok bool) {
	if ent, has := c.items[key]; has {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*lruItem[K, V]) == nil {
			return
		}
		ok = true
		value = ent.Value.(*lruItem[K, V]).value
		return
	}
	return
}

func (c *LRU[K, V]) Remove(key K) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		present = true
		return
	}
	return
}

func (c *LRU[K, V]) Keys() []K {
	keys := make([]K, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*lruItem[K, V]).key
		i++
	}
	return keys
}

func (c *LRU[K, V]) Len() int {
	return c.evictList.Len()
}

func (c *LRU[K, V]) Resize(size int64) (evicted int64) {
	evicted = int64(c.Len()) - size
	if evicted < 0 {
		evicted = 0
	}
	for i := int64(0); i < evicted; i++ {
		c.removeOldest()
	}
	c.size = size
	return
}

func (c *LRU[K, V]) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

func (c *LRU[K, V]) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*lruItem[K, V])
	delete(c.items, kv.key)
}
