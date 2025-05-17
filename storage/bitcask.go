package storage

import (
	"fmt"
	"log"
)

type BitCask struct {
	log    *Log
	keyDir *KeyDir
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
