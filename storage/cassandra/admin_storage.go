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

func (s *cassAdminStorage) ReadWriteTransaction(ctx context.Context, f storage.AdminTXFunc) error {
	glog.Infof("cassAdminStorage.ReadWriteTransaction")
	tx, err := s.beginInternal(ctx)
	if err != nil {
		glog.Infof("cassAdminStorage.ReadWriteTransaction:beginInternal err=%v", err)
		return err
	}
	defer tx.Close()
	if err := f(ctx, tx); err != nil {
		glog.Infof("cassAdminStorage.ReadWriteTransaction:f() err=%v", err)
		return err
	}
	return tx.Commit()
}

func (s *cassAdminStorage) CheckDatabaseAccessible(context.Context) error {
	if s.ks.Name() == "" {
		return errors.New("cassAdminStorage.CheckDatabaseAccessible: Cassandra not ready")
	}
	return nil
}

func (s *cassAdminStorage) Snapshot(ctx context.Context) (storage.ReadOnlyAdminTX, error) {
	glog.Infof("cassAdminStorage.Snapshot")
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
	glog.Infof("cassAdminTX.Rollback: no-op")
  return nil
}

func (t *cassAdminTX) IsClosed() bool {
  glog.Infof("cassAdminTX.Commit")
  return false
}

func (t *cassAdminTX) Close() error {
	glog.Infof("cassAdminTX.Close")
  return errors.New("cassAdminTX.Close: not implemented")
}

func (t *cassAdminTX) GetTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
	glog.Infof("Admin.GetTree: treeID=%d", treeID)
	treesTable := t.ks.Table("trees", &tree{}, gocassa.Keys{
		PartitionKeys: []string{"tree_id"},
  }).WithOptions(gocassa.Options{TableName: "trees"})
	treeResult := tree{}
	if err := treesTable.Where(gocassa.Eq("tree_id", treeID)).ReadOne(&treeResult).Run(); err != nil {
		return nil, err
	}
	glog.Infof("GetTree: read tree %v", treeResult)
	return &trillian.Tree{
		TreeId: treeID,
		DisplayName: treeResult.DisplayName,
	}, nil
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
	glog.Infof("Admin.ListTrees: includeDeleted=%t", includeDeleted)
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

func (t *cassAdminTX) defaultGroupID(ctx context.Context) (string, error) {
	groupsByNameTable := t.ks.Table("groups_by_name", &group{}, gocassa.Keys{
		PartitionKeys: []string{"group_name"},
  })
	groupsByNameTable = groupsByNameTable.WithOptions(gocassa.Options{TableName: "groups_by_name"})
	groupResult := group{}
	if err := groupsByNameTable.Where(gocassa.Eq("group_name", "_default")).ReadOne(&groupResult).Run(); err != nil {
		return "", err
	}
	glog.Infof("read group %v", groupResult)
	return groupResult.ID, nil
}

func (t *cassAdminTX) ListTreeIDs(ctx context.Context, includeDeleted bool) ([]int64, error) {
	glog.Infof("Admin.ListTreeIDs: includeDeleted=%t", includeDeleted)
	defGroupID, err := t.defaultGroupID(ctx)
	if err != nil {
		return nil, err
	}

  treesByGroupIDTable := t.ks.MultimapTable("trees_by_group_id", "group_id", "tree_id", &treeGroup{})
	treesByGroupIDTable = treesByGroupIDTable.WithOptions(gocassa.Options{TableName: "trees_by_group_id"})
	treeGroupsResult := []treeGroup{}
	if err = treesByGroupIDTable.List(defGroupID, nil, 0, &treeGroupsResult).Run(); err != nil {
    return nil, err
  }

  treeIDs := make([]int64, len(treeGroupsResult))
	for i, tg := range treeGroupsResult {
		treeIDs[i] = tg.TreeID
	}
	glog.Infof("read treeIDs %v", treeIDs)
  return treeIDs, nil
}

func (t *cassAdminTX) CreateTree(ctx context.Context, trilTree *trillian.Tree) (*trillian.Tree, error) {
//  return nil, errors.New("cassAdminTX.CreateTree: not implemented")
	/*
	CreateTree(tree:<
	  tree_state:ACTIVE
		tree_type:LOG
		hash_strategy:RFC6962_SHA256
		hash_algorithm:SHA256
		signature_algorithm:ECDSA
		private_key:<
		  type_url:"type.googleapis.com/keyspb.PrivateKey"
			value:"\ny0w\002\001\001\004 \350\260A\3519N\017(\256\257\237\317\344NkJ\232ah\031>QU\304Nj$\226\25731\367\240\n\006\010*\206H\316=\003\001\007\241D\003B\000\004\313\t\326\211\303DC\306\347\334e\334\032\220>\266%8T\276\364Z\317q{8\020>\270=\351\201\254\026\261z)\201\272\362*\262\0349P\373\201V\270\247\306)q\347\336\212\025\264\037\351}\254\033\345"
		>
		max_root_duration:<seconds:3600 >
	> )
	*/
	glog.Infof("Admin.CreateTree: tree=%v", trilTree)
	defGroupID, err := t.defaultGroupID(ctx)
	if err != nil {
		return nil, err
	}

	treesByGroupIDTable := t.ks.MultimapTable("trees_by_group_id", "group_id", "tree_id", &treeGroup{})
	treesByGroupIDTable = treesByGroupIDTable.WithOptions(gocassa.Options{TableName: "trees_by_group_id"})
	treesTable := t.ks.Table("trees", &tree{}, gocassa.Keys{
		PartitionKeys: []string{"tree_id"},
  })
	treesTable = treesTable.WithOptions(gocassa.Options{TableName: "trees"})

	if err := treesByGroupIDTable.Set(treeGroup{
		TreeID:  12,
		GroupID: defGroupID,
	}).Add(treesTable.Set(tree{
		TreeID:      12,
		DisplayName: "John",
	})).RunLoggedBatchWithContext(ctx); err != nil {
		return nil, err
	}
	return trilTree, nil
}

func (t *cassAdminTX) UpdateTree(ctx context.Context, treeID int64, updateFunc func(*trillian.Tree)) (*trillian.Tree, error) {
	glog.Infof("Admin.UpdateTree: treeID=%d", treeID)
  return nil, errors.New("cassAdminTX.UpdateTree: not implemented")
}

func (t *cassAdminTX) SoftDeleteTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
	glog.Infof("Admin.SoftDeleteTree: treeID=%d", treeID)
  return nil, errors.New("cassAdminTX.SoftDeleteTree: not implemented")
}

func (t *cassAdminTX) UndeleteTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
	glog.Infof("Admin.UndeleteTree: treeID=%d", treeID)
  return nil, errors.New("cassAdminTX.UndeleteTree: not implemented")
}

func (t *cassAdminTX) HardDeleteTree(ctx context.Context, treeID int64) error {
	glog.Infof("Admin.HardDeleteTree: treeID=%d", treeID)
  return errors.New("cassAdminTX.HardDeleteTree: not implemented")
}
