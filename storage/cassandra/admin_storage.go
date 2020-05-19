package cassandra

import (
	"context"
	"errors"

	"github.com/google/trillian/storage"
	"github.com/monzo/gocassa"
)

// NewAdminStorage returns a storage.AdminStorage implementation
func NewAdminStorage(ks gocassa.KeySpace) storage.AdminStorage {
	return &cassAdminStorage{ks: ks}
}

type cassAdminStorage struct {
	ks gocassa.KeySpace
}

func (s *cassAdminStorage) ReadWriteTransaction(context.Context, storage.AdminTXFunc) error {
	return errors.New("cassAdminStorage.ReadWriteTransaction: not implemented")
}

func (s *cassAdminStorage) CheckDatabaseAccessible(context.Context) error {
	if s.ks.Name() == "" {
		return errors.New("cassAdminStorage.CheckDatabaseAccessible: Cassandra not ready")
	}
	return nil
}

func (s *cassAdminStorage) Snapshot(context.Context) (storage.ReadOnlyAdminTX, error) {
	return nil, errors.New("cassAdminStorage.Snapshot: not implemented")
}
