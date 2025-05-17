package storage

import "github.com/google/btree"

type KV struct {
	Key   []byte
	Value []byte
}

func (k *KV) Less(than btree.Item) bool {
	return string(k.Key) < string(than.(*KV).Key)
}

type MemoryStorge struct {
	tree *btree.BTree
}

func NewMemoryStorage() *MemoryStorge {
	return &MemoryStorge{
		tree: btree.New(2),
	}
}

func (m *MemoryStorge) Delete(key []byte) {
	m.tree.Delete(&KV{Key: key})
}

func (m *MemoryStorge) Flush() {

}

func (m *MemoryStorge) Get(key []byte) ([]byte, bool) {
	item := m.tree.Get(&KV{Key: key})
	if item == nil {
		return nil, false
	}
	return item.(*KV).Value, true
}

func (m *MemoryStorge) Set(key, value []byte) {
	m.tree.ReplaceOrInsert(&KV{Key: key, Value: value})
}

func (m *MemoryStorge) Status() *Status {
	var size uint64
	m.tree.Ascend(func(i btree.Item) bool {
		kv := i.(*KV)
		size += uint64(len(kv.Key) + len(kv.Value))
		return true
	})
	return &Status{
		Name:         "memory",
		Keys:         uint64(m.tree.Len()),
		Size:         0,
		DiskSize:     0,
		LiveDiskSize: 0,
	}
}
