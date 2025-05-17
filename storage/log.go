package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

type Log struct {
	File *os.File
	Path string
	Lock *flock.Flock
}

func NewLog(path string) (*Log, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	lock := flock.New(path + ".lock")
	locked, err := lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock file: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("file %s is already in use", path)
	}

	return &Log{
		File: file,
		Path: path,
		Lock: lock,
	}, nil
}

func (l *Log) BuildKeyDir() (*KeyDir, error) {
	r := bufio.NewReader(l.File)

	stat, err := l.File.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}
	fileLen := stat.Size()

	offset := int64(0)
	_, err = l.File.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek file: %w", err)
	}

	keyDir := NewKeyDir()
	lenBuf := make([]byte, 4)

	for offset < fileLen {
		key, valueLoc, err := func() ([]byte, *ValueLocation, error) {
			if _, err := io.ReadFull(r, lenBuf); err != nil {
				return nil, nil, err
			}
			keyLen := binary.BigEndian.Uint32(lenBuf)

			if _, err := io.ReadFull(r, lenBuf); err != nil {
				return nil, nil, err
			}
			valLen := int32(binary.BigEndian.Uint32(lenBuf))

			var valLoc *ValueLocation
			if valLen >= 0 {
				valLoc = &ValueLocation{
					Offset: offset + 8 + int64(keyLen),
					Length: valLen,
				}
			}

			key := make([]byte, keyLen)
			if _, err := io.ReadFull(r, key); err != nil {
				return nil, nil, err
			}

			if valLoc != nil {
				end := valLoc.end()
				if end > fileLen {
					return nil, nil, io.ErrUnexpectedEOF
				}
				if _, err := l.File.Seek(int64(valLoc.Length), io.SeekCurrent); err != nil {
					return nil, nil, err
				}
			}

			return key, valLoc, nil
		}()

		switch {
		case err == nil && valueLoc != nil:
			keyDir.Set(key, *valueLoc)
		case err == nil:
			keyDir.Delete(key)
		case errors.Is(err, io.ErrUnexpectedEOF):
			fmt.Printf("Found incomplete entry at Offset %d, truncating file\n", offset)
			if err := l.File.Truncate(offset); err != nil {
				return nil, err
			}
			break
		case err != nil:
			return nil, err
		}

		valueLen := 0
		if valueLoc != nil {
			valueLen = int(valueLoc.Length)
		}
		offset += 8 + int64(len(key)) + int64(valueLen)
	}

	return keyDir, nil
}

func (l *Log) ReadValue(loc ValueLocation) ([]byte, error) {
	value := make([]byte, loc.Length)
	if _, err := l.File.Seek(loc.Offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek failed: %w", err)
	}
	if _, err := io.ReadFull(l.File, value); err != nil {
		return nil, fmt.Errorf("read value failed: %w", err)
	}
	return value, nil
}

func (l *Log) WriteEntry(key []byte, value []byte) (ValueLocation, error) {
	valLen := -1
	if value != nil {
		valLen = len(value)
	}

	length := 8 + len(key) + max(0, valLen)
	offset, err := l.File.Seek(0, io.SeekEnd)
	if err != nil {
		return ValueLocation{}, fmt.Errorf("seek end: %w", err)
	}

	bufWriter := bufio.NewWriterSize(l.File, length)

	if err := binary.Write(bufWriter, binary.BigEndian, uint32(len(key))); err != nil {
		return ValueLocation{}, fmt.Errorf("write key len: %w", err)
	}

	if err := binary.Write(bufWriter, binary.BigEndian, int32(valLen)); err != nil {
		return ValueLocation{}, fmt.Errorf("write value len: %w", err)
	}

	if _, err := bufWriter.Write(key); err != nil {
		return ValueLocation{}, fmt.Errorf("write key: %w", err)
	}

	if valLen > 0 {
		if _, err := bufWriter.Write(value); err != nil {
			return ValueLocation{}, fmt.Errorf("write value: %w", err)
		}
	}

	if err := bufWriter.Flush(); err != nil {
		return ValueLocation{}, fmt.Errorf("flush error: %w", err)
	}

	return ValueLocation{
		Offset: offset + 8 + int64(len(key)),
		Length: int32(max(0, valLen)),
	}, nil
}
