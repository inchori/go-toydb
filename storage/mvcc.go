package storage

import (
	"encoding/binary"
	"sync"
)

const (
	mvccNextVersionKeyTag    = byte(0x00)
	mvccTxnActiveKeyTag      = byte(0x01)
	mvccTxnActiveSnapshotKey = byte(0x02)
	mvccTxnWriteKeyTag       = byte(0x03)
	mvccVersionedKeyTag      = byte(0x04)
	mvccUnversionedKeyTag    = byte(0x05)
)

var (
	mvccNextVersionKey     = []byte{mvccNextVersionKeyTag}
	mvccTxnActiveKeyPrefix = []byte{mvccTxnActiveKeyTag}
	mvccVersionedKeyPrefix = []byte{mvccVersionedKeyTag}
)

type MVCC struct {
	Engine Engine
	mu     sync.RWMutex
}

type MVCCTx struct {
	mvcc     *MVCC
	Version  Version
	ReadOnly bool
	Active   map[Version]struct{}
}

type Version uint64

func NewMVCC(engine Engine) *MVCC {
	return &MVCC{
		Engine: engine,
	}
}

func (m *MVCC) Begin(readOnly bool) (*MVCCTx, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var version Version
	if readOnly {
		if versionBytes, err := m.Engine.Get(mvccNextVersionKey); err == nil {
			version = Version(binary.BigEndian.Uint64(versionBytes)) - 1
		} else {
			version = Version(0)
		}
	} else {
		version = Version(1)
		if versionBytes, err := m.Engine.Get(mvccNextVersionKey); err == nil {
			version = Version(binary.BigEndian.Uint64(versionBytes))
		}

		nextVersionBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(nextVersionBytes, uint64(version+1))
		if err := m.Engine.Set(mvccNextVersionKey, nextVersionBytes); err != nil {
			return nil, err
		}
	}

	active, err := m.scanActive()
	if err != nil {
		return nil, err
	}

	if !readOnly {
		if len(active) > 0 {
			activeSnapshot := m.encodeVersionSet(active)
			snapshotKey := m.makeTxnActiveSnapshotKey(version)
			if err := m.Engine.Set(snapshotKey, activeSnapshot); err != nil {
				return nil, err
			}
		}

		txnActiveKey := m.makeTxnActiveKey(version)
		if err := m.Engine.Set(txnActiveKey, []byte{}); err != nil {
			return nil, err
		}
	}

	return &MVCCTx{
		mvcc:     m,
		Version:  version,
		ReadOnly: readOnly,
		Active:   active,
	}, nil
}

func (m *MVCC) BeginAsOf(version Version) (*MVCCTx, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var active map[Version]struct{}
	snapshotKey := m.makeTxnActiveSnapshotKey(version)
	if snapshotBytes, err := m.Engine.Get(snapshotKey); err == nil && len(snapshotBytes) > 0 {
		active = m.decodeVersionSet(snapshotBytes)
	} else {
		active = make(map[Version]struct{})
	}

	return &MVCCTx{
		mvcc:     m,
		Version:  version,
		ReadOnly: true,
		Active:   active,
	}, nil
}

func (m *MVCC) Resume(version Version, readOnly bool, active map[Version]struct{}) (*MVCCTx, error) {
	return &MVCCTx{
		mvcc:     m,
		Version:  version,
		ReadOnly: readOnly,
		Active:   active,
	}, nil
}

func (tx *MVCCTx) IsReadOnly() bool {
	return tx.ReadOnly
}

func (tx *MVCCTx) IsVisible(version Version) bool {
	if _, ok := tx.Active[version]; ok {
		return false
	} else if tx.ReadOnly {
		return version < tx.Version
	} else {
		return version <= tx.Version
	}
}

func (tx *MVCCTx) Commit() error {
	if tx.ReadOnly {
		return nil
	}

	tx.mvcc.mu.Lock()
	defer tx.mvcc.mu.Unlock()

	txnActiveKey := tx.mvcc.makeTxnActiveKey(tx.Version)
	return tx.mvcc.Engine.Delete(txnActiveKey)
}

func (tx *MVCCTx) Rollback() error {
	if tx.ReadOnly {
		return nil
	}

	tx.mvcc.mu.Lock()
	defer tx.mvcc.mu.Unlock()

	txnActiveKey := tx.mvcc.makeTxnActiveKey(tx.Version)
	return tx.mvcc.Engine.Delete(txnActiveKey)
}

func (m *MVCC) scanActive() (map[Version]struct{}, error) {
	active := make(map[Version]struct{})

	scanner := m.Engine.Scan(mvccTxnActiveKeyPrefix, nil)
	defer scanner.Close()

	for scanner.Next() {
		key := scanner.Key()
		if len(key) < 9 || key[0] != mvccTxnActiveKeyTag {
			continue
		}

		version := Version(binary.BigEndian.Uint64(key[1:9]))
		active[version] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return active, nil
}

func (m *MVCC) encodeVersionSet(active map[Version]struct{}) []byte {
	if len(active) == 0 {
		return []byte{}
	}

	versions := make([]Version, 0, len(active))
	for version := range active {
		versions = append(versions, version)
	}

	for i := 0; i < len(versions); i++ {
		for j := i + 1; j < len(versions); j++ {
			if versions[i] > versions[j] {
				versions[i], versions[j] = versions[j], versions[i]
			}
		}
	}

	result := make([]byte, 4+len(versions)*8)

	binary.BigEndian.PutUint32(result[0:4], uint32(len(versions)))

	for i, version := range versions {
		offset := 4 + i*8
		binary.BigEndian.PutUint64(result[offset:offset+8], uint64(version))
	}

	return result
}

func (m *MVCC) decodeVersionSet(data []byte) map[Version]struct{} {
	active := make(map[Version]struct{})

	if len(data) < 4 {
		return active
	}

	count := binary.BigEndian.Uint32(data[0:4])

	for i := uint32(0); i < count; i++ {
		offset := 4 + i*8
		if offset+8 > uint32(len(data)) {
			break
		}
		version := Version(binary.BigEndian.Uint64(data[offset : offset+8]))
		active[version] = struct{}{}
	}

	return active
}

func (m *MVCC) makeTxnActiveKey(version Version) []byte {
	key := make([]byte, 9)
	key[0] = mvccTxnActiveKeyTag
	binary.BigEndian.PutUint64(key[1:], uint64(version))
	return key
}

func (m *MVCC) makeTxnActiveSnapshotKey(version Version) []byte {
	key := make([]byte, 9)
	key[0] = mvccTxnActiveSnapshotKey
	binary.BigEndian.PutUint64(key[1:], uint64(version))
	return key
}

func (m *MVCC) GetUnversioned(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Engine.Get(key)
}

func (m *MVCC) SetUnversioned(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Engine.Set(key, value)
}
