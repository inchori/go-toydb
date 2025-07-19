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

func (m *MemoryStorage) Delete(key []byte) {
	m.tree.Delete(&item{key: key})
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

func (m *MemoryStorage) Set(key, value []byte) {
	m.tree.ReplaceOrInsert(&item{key: key, value: value})
}

func (m *MemoryStorage) Status() *EngineStatus {
	status := &EngineStatus{
		Name: "MemoryStorage",
		keys: uint64(m.tree.Len()),
	}

	m.tree.Ascend(func(i btree.Item) bool {
		status.Size += uint64(len(i.(*item).key) + len(i.(*item).value))
		return true
	})

	status.DiskSize = 0
	status.LiveDiskSize = 0

	return status
}
