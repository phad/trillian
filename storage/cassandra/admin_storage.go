package cassandra

import (
	"context"
  "errors"

	"github.com/google/trillian/storage"
)


// NewAdminStorage returns a storage.AdminStorage implementation
func NewAdminStorage() storage.AdminStorage {
	return &cassAdminStorage{}
}

type cassAdminStorage struct {
  // TODO(phad): add cass DB connection.
}

func (s *cassAdminStorage) ReadWriteTransaction(context.Context, storage.AdminTXFunc) error {
  return errors.New("adminStorage.ReadWriteTransaction: not implemented")
}

func (s *cassAdminStorage) CheckDatabaseAccessible(context.Context) error {
  return errors.New("adminStorage.CheckDatabaseAccessible: not implemented")
}

func (s *cassAdminStorage) Snapshot(context.Context) (storage.ReadOnlyAdminTX, error) {
  return nil, errors.New("adminStorage.Snapshot: not implemented")
}
