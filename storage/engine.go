package storage

type Engine interface {
	Delete(key []byte) error
	Flush()
	Get(key, value []byte) error
	Set(key, value []byte) error
	Status() (Status, error)
	//TODO: ScanIterator
}
