package wal

import (
	"container/list"
)

type LRU struct {
	size      int64
	evictList *list.List
	items     map[interface{}]*list.Element
}

type lruItem struct {
	key   uint64
	value Entry
}

func NewLRU(size uint32) (lru *LRU) {
	lru = &LRU{
		size:      int64(size),
		evictList: list.New(),
		items:     make(map[interface{}]*list.Element),
	}
	return
}

func (c *LRU) Purge() {
	for k := range c.items {
		delete(c.items, k)
	}
	c.evictList.Init()
}

func (c *LRU) Add(key uint64, value Entry) (evicted bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*lruItem).value = value
		return
	}

	ent := &lruItem{key, value}
	c.items[key] = c.evictList.PushFront(ent)

	evicted = int64(c.evictList.Len()) > c.size
	if evicted {
		c.removeOldest()
	}
	return
}

func (c *LRU) Get(key uint64) (value Entry, ok bool) {
	if ent, has := c.items[key]; has {
		c.evictList.MoveToFront(ent)
		if ent.Value.(*lruItem) == nil {
			return
		}
		ok = true
		value = ent.Value.(*lruItem).value
		return
	}
	return
}

func (c *LRU) Remove(key uint64) (present bool) {
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		present = true
		return
	}
	return
}

func (c *LRU) Keys() []uint64 {
	keys := make([]uint64, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = ent.Value.(*lruItem).key
		i++
	}
	return keys
}

func (c *LRU) Len() int {
	return c.evictList.Len()
}

func (c *LRU) Resize(size int64) (evicted int64) {
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

func (c *LRU) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

func (c *LRU) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*lruItem)
	delete(c.items, kv.key)
}
