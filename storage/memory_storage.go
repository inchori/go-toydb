package storage

import (
	"bytes"
	"fmt"

	"github.com/google/btree"
)

type item struct {
	key   []byte
	value []byte
}

func (i *item) Less(b btree.Item) bool {
	return bytes.Compare(i.key, b.(*item).key) < 0
}

type MemoryStorage struct {
	tree *btree.BTree
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{tree: btree.New(32)}
}

func (m *MemoryStorage) Delete(key []byte) error {
	m.tree.Delete(&item{key: key})
	return nil
}

func (m *MemoryStorage) Flush() error {
	return nil
}

func (m *MemoryStorage) Get(key []byte) ([]byte, error) {
	if result := m.tree.Get(&item{key: key}); result != nil {
		return result.(*item).value, nil
	}
	return nil, fmt.Errorf("key not found: %s", key)
}

func (m *MemoryStorage) Set(key, value []byte) error {
	m.tree.ReplaceOrInsert(&item{key: key, value: value})
	return nil
}

func (m *MemoryStorage) Status() *EngineStatus {
	status := &EngineStatus{
		Name: "MemoryStorage",
		Keys: uint64(m.tree.Len()),
	}

	m.tree.Ascend(func(i btree.Item) bool {
		status.Size += uint64(len(i.(*item).key) + len(i.(*item).value))
		return true
	})

	status.DiskSize = 0
	status.LiveDiskSize = 0

	return status
}

func (m *MemoryStorage) Scan(start, end []byte) ScanIterator {
	var items []*item

	m.tree.Ascend(func(i btree.Item) bool {
		it := i.(*item)
		if start != nil && bytes.Compare(it.key, start) < 0 {
			return true
		}
		if end != nil && bytes.Compare(it.key, end) >= 0 {
			return false
		}
		items = append(items, it)
		return true
	})

	return &memoryScanIterator{items: items, index: -1}
}

type memoryScanIterator struct {
	items []*item
	index int
	err   error
}

func (it *memoryScanIterator) Next() bool {
	it.index++
	return it.index < len(it.items)
}

func (it *memoryScanIterator) Key() []byte {
	if it.index < 0 || it.index >= len(it.items) {
		return nil
	}
	return it.items[it.index].key
}

func (it *memoryScanIterator) Value() []byte {
	if it.index < 0 || it.index >= len(it.items) {
		return nil
	}
	return it.items[it.index].value
}

func (it *memoryScanIterator) Err() error {
	return it.err
}

func (it *memoryScanIterator) Close() {
}
