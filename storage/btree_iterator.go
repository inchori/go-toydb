package storage

import (
	"bytes"
	"github.com/google/btree"
)

type BTreeScanIterator struct {
	items []*KV
	index int
	err   error
}

func NewBtreeScanIterator(tree *btree.BTree, start, end []byte) *BTreeScanIterator {
	var items []*KV

	tree.Ascend(func(item btree.Item) bool {
		kv := item.(*KV)
		if bytes.Compare(kv.Key, start) < 0 {
			return true
		}
		if end != nil && bytes.Compare(kv.Key, end) >= 0 {
			return false
		}
		items = append(items, kv)
		return true
	})

	return &BTreeScanIterator{items: items, index: -1}
}

func (bt *BTreeScanIterator) Next() bool {
	bt.index++
	return bt.index < len(bt.items)
}

func (bt *BTreeScanIterator) Key() []byte {
	if bt.index < 0 || bt.index >= len(bt.items) {
		return nil
	}
	return bt.items[bt.index].Key
}

func (bt *BTreeScanIterator) Value() []byte {
	if bt.index < 0 || bt.index >= len(bt.items) {
		return nil
	}
	return bt.items[bt.index].Value
}

func (bt *BTreeScanIterator) Err() error {
	return bt.err
}
