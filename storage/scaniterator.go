package storage

type ScanIterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
}
