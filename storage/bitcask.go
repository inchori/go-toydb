package storage

import (
	"fmt"
	"log"
	"os"

	"github.com/google/btree"
)

type BitCask struct {
	Log    *Log
	KeyDir *KeyDir
}

func NewBitCask(path string) (*BitCask, error) {
	log.Printf("Opening database %s", path)

	logFile, err := NewLog(path)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}

	keyDir, err := logFile.BuildKeyDir()
	if err != nil {
		return nil, fmt.Errorf("build keydir: %w", err)
	}

	log.Printf("Indexed %d live keys in %s", keyDir.Len(), path)

	return &BitCask{
		Log:    logFile,
		KeyDir: keyDir,
	}, nil
}

func (b *BitCask) Delete(key []byte) error {
	_, err := b.Log.WriteEntry(key, nil) // nil == tombstone
	if err != nil {
		return fmt.Errorf("write tombstone: %w", err)
	}
	b.KeyDir.Delete(key)
	return nil
}

func (b *BitCask) Flush() error {
	if err := b.Log.File.Sync(); err != nil {
		return fmt.Errorf("fsync failed: %w", err)
	}
	return nil
}

func (b *BitCask) Get(key []byte) ([]byte, error) {
	valLoc, ok := b.KeyDir.Get(key)
	if !ok {
		return nil, nil
	}
	value, err := b.Log.ReadValue(valLoc)
	if err != nil {
		return nil, fmt.Errorf("read value: %w", err)
	}
	return value, nil
}

func (b *BitCask) Set(key []byte, value []byte) error {
	valLoc, err := b.Log.WriteEntry(key, value)
	if err != nil {
		return fmt.Errorf("write entry: %w", err)
	}
	b.KeyDir.Set(key, valLoc)
	return nil
}

func (b *BitCask) Status() (Status, error) {
	var size int64
	b.KeyDir.tree.Ascend(func(item btree.Item) bool {
		bkv := item.(*BKV)
		size += int64(len(bkv.Key) + int(bkv.Value.Length))
		return true
	})

	keys := int64(b.KeyDir.Len())
	fileInfo, err := b.Log.File.Stat()
	if err != nil {
		return Status{}, fmt.Errorf("stat file: %w", err)
	}
	diskSize := fileInfo.Size()
	liveDiskSize := size + 8*keys

	return Status{
		Name:         "bitcask",
		Keys:         keys,
		Size:         size,
		DiskSize:     diskSize,
		LiveDiskSize: liveDiskSize,
	}, nil
}

func (b *BitCask) Compact() error {
	newPath := b.Log.Path + ".new"
	newLog, err := NewLog(newPath)
	if err != nil {
		return fmt.Errorf("create new log: %w", err)
	}

	if err := newLog.File.Truncate(0); err != nil {
		return fmt.Errorf("truncate new log: %w", err)
	}

	newKeyDir := NewKeyDir()

	b.KeyDir.tree.Ascend(func(item btree.Item) bool {
		kv := item.(*BKV)
		value, err := b.Log.ReadValue(kv.Value)
		if err != nil {
			panic(fmt.Errorf("read value: %w", err))
		}

		newLoc, err := newLog.WriteEntry(kv.Key, value)
		if err != nil {
			panic(fmt.Errorf("write entry: %w", err))
		}

		newKeyDir.Set(kv.Key, newLoc)
		return true
	})

	if err := os.Rename(newLog.Path, b.Log.Path); err != nil {
		return fmt.Errorf("rename log: %w", err)
	}

	newLog.Path = b.Log.Path
	b.Log = newLog
	b.KeyDir = newKeyDir

	return nil
}

//TODO: Drop BitCask Engine
