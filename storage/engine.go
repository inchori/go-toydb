package storage

type Engine interface {
	Delete(key []byte) error
	Flush() error
	Get(key []byte) ([]byte, error)
	Set(key, value []byte)
	Status() *EngineStatus
}
