package cassandra

import (
	"context"
	"errors"

	"github.com/golang/glog"
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
	return &cassAdminTX{
      ks: s.ks,
  }, nil
}

type cassAdminTX struct {
  ks gocassa.KeySpace
}

func (t *cassAdminTX) Commit() error {
	glog.Infof("cassAdminTX.Commit: no-op")
  return nil
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

type group struct {
  ID   string `cql:"group_id"`
	Name string `cql:"group_name"`
}

type treeGroup struct {
	TreeID int64 `cql:"tree_id"`
	GroupID string `cql:"group_id"`
}

type tree struct {
  TreeID int64 `cql:"tree_id"`
	DisplayName string `cql:"display_name"`
}

func (t *cassAdminTX) ListTrees(ctx context.Context, includeDeleted bool) ([]*trillian.Tree, error) {
	treeIDs, err := t.ListTreeIDs(ctx, includeDeleted)
	if err != nil {
		return nil, err
	}
	result := make([]*trillian.Tree, len(treeIDs))
	treesTable := t.ks.Table("trees", &tree{}, gocassa.Keys{
		PartitionKeys: []string{"tree_id"},
  })
	treesTable = treesTable.WithOptions(gocassa.Options{TableName: "trees"})
	treeResult := tree{}
	for i, tID := range treeIDs {
		if err := treesTable.Where(gocassa.Eq("tree_id", tID)).ReadOne(&treeResult).Run(); err != nil {
			return nil, err
		}
		glog.Infof("read tree %v", treeResult)
		result[i] = &trillian.Tree{
			TreeId: tID,
			DisplayName: treeResult.DisplayName,
		}
	}
	return result, nil
}

func (t *cassAdminTX) ListTreeIDs(ctx context.Context, includeDeleted bool) ([]int64, error) {
	groupsByNameTable := t.ks.Table("groups_by_name", &group{}, gocassa.Keys{
		PartitionKeys: []string{"group_name"},
  })
	groupsByNameTable = groupsByNameTable.WithOptions(gocassa.Options{TableName: "groups_by_name"})
	groupResult := group{}
	if err := groupsByNameTable.Where(gocassa.Eq("group_name", "_default")).ReadOne(&groupResult).Run(); err != nil {
		return nil, err
	}
	glog.Infof("read group %v", groupResult)

  treesByGroupIDTable := t.ks.MultimapTable("trees_by_group_id", "group_id", "tree_id", &treeGroup{})
	treesByGroupIDTable = treesByGroupIDTable.WithOptions(gocassa.Options{TableName: "trees_by_group_id"})
	treeGroupsResult := []treeGroup{}
	err := treesByGroupIDTable.List(groupResult.ID, nil, 0, &treeGroupsResult).Run()
	if err != nil {
    return nil, err
  }

  treeIDs := make([]int64, len(treeGroupsResult))
	for i, tg := range treeGroupsResult {
		treeIDs[i] = tg.TreeID
	}
	glog.Infof("read treeIDs %v", treeIDs)
  return treeIDs, nil
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
