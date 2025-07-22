package storage

type ScanIterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
	Close()
}

type Engine interface {
	Delete(key []byte) error
	Flush() error
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Scan(start, end []byte) ScanIterator
	Status() *EngineStatus
}
