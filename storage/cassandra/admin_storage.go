package cassandra

import (
	"context"
	"errors"

  "github.com/google/trillian"
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

func (s *cassAdminStorage) Snapshot(ctx context.Context) (storage.ReadOnlyAdminTX, error) {
	return s.beginInternal(ctx)
}

func (s *cassAdminStorage) beginInternal(ctx context.Context) (storage.AdminTX, error) {
	return &cassAdminTX{}, nil
}

type cassAdminTX struct {}

func (t *cassAdminTX) Commit() error {
  return errors.New("cassAdminTX.Commit: not implemented")
}

func (t *cassAdminTX) Rollback() error {
  return errors.New("cassAdminTX.Rollback: not implemented")
}

func (t *cassAdminTX) IsClosed() bool {
  panic("cassAdminTX.Commit: not implemented")
  return false
}

func (t *cassAdminTX) Close() error {
  return errors.New("cassAdminTX.Close: not implemented")
}

func (t *cassAdminTX) GetTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
  return nil, errors.New("cassAdminTX.GetTree: not implemented")
}

func (t *cassAdminTX) ListTrees(ctx context.Context, includeDeleted bool) ([]*trillian.Tree, error) {
  return nil, errors.New("cassAdminTX.ListTrees: not implemented")
}

func (t *cassAdminTX) ListTreeIDs(ctx context.Context, includeDeleted bool) ([]int64, error) {
  return nil, errors.New("cassAdminTX.ListTreeIDs: not implemented")
}

func (t *cassAdminTX) CreateTree(ctx context.Context, tree *trillian.Tree) (*trillian.Tree, error) {
  return nil, errors.New("cassAdminTX.CreateTree: not implemented")
}

func (t *cassAdminTX) UpdateTree(ctx context.Context, treeID int64, updateFunc func(*trillian.Tree)) (*trillian.Tree, error) {
  return nil, errors.New("cassAdminTX.UpdateTree: not implemented")
}

func (t *cassAdminTX) SoftDeleteTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
  return nil, errors.New("cassAdminTX.SoftDeleteTree: not implemented")
}

func (t *cassAdminTX) UndeleteTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
  return nil, errors.New("cassAdminTX.UndeleteTree: not implemented")
}

func (t *cassAdminTX) HardDeleteTree(ctx context.Context, treeID int64) error {
  return errors.New("cassAdminTX.HardDeleteTree: not implemented")
}
