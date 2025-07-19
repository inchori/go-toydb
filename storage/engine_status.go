package storage

type EngineStatus struct {
	Name         string
	keys         uint64
	Size         uint64
	DiskSize     uint64
	LiveDiskSize uint64
}

func (e *EngineStatus) GarbageDiskSize() uint64 {
	return e.DiskSize - e.LiveDiskSize
}

func (e *EngineStatus) GarbageDiskPercent() float64 {
	if e.DiskSize == 0 {
		return 0.0
	}
	return float64(e.GarbageDiskSize()) / float64(e.DiskSize) * 100.0
}
