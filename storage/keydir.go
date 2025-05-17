package storage

import (
	"bytes"

	"github.com/google/btree"
)

type ValueLocation struct {
	Offset int64
	Length int32
}

func (v *ValueLocation) end() int64 {
	return v.Offset + int64(v.Length)
}

type BKV struct {
	Key   []byte
	Value ValueLocation
}

func (k *BKV) Less(b btree.Item) bool {
	return bytes.Compare(k.Key, b.(*BKV).Key) < 0
}

type KeyDir struct {
	tree *btree.BTree
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		tree: btree.New(2),
	}
}

func (k *KeyDir) Len() int {
	return k.tree.Len()
}

func (k *KeyDir) Set(key []byte, value ValueLocation) {
	k.tree.ReplaceOrInsert(&BKV{Key: append([]byte(nil), key...), Value: value})
}

func (k *KeyDir) Delete(key []byte) {
	k.tree.Delete(&BKV{Key: append([]byte(nil), key...)})
}
