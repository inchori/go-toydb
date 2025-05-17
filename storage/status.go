package storage

type Status struct {
	Name         string
	Keys         int64
	Size         int64
	DiskSize     int64
	LiveDiskSize int64
}

func (s *Status) GarbageDiskSize() int64 {
	return s.DiskSize - s.LiveDiskSize
}

func (s *Status) GarbageDiskPercent() float64 {
	if s.DiskSize == 0 {
		return 0.0
	}
	return (float64(s.GarbageDiskSize()) / float64(s.DiskSize)) * 100.0
}
