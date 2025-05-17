package storage

type Status struct {
	Name         string
	Keys         uint64
	Size         uint64
	DiskSize     uint64
	LiveDiskSize uint64
}

func (s *Status) GarbageDiskSize() uint64 {
	return s.DiskSize - s.LiveDiskSize
}

func (s *Status) GarbageDiskPercent() float64 {
	if s.DiskSize == 0 {
		return 0.0
	}
	return (float64(s.GarbageDiskSize()) / float64(s.DiskSize)) * 100.0
}
