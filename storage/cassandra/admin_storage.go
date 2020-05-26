package cassandra

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
  "github.com/google/trillian"
	spb "github.com/google/trillian/crypto/sigpb"
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

func cassTreeToTrillianTree(cassTree *tree) (*trillian.Tree, error) {
	trilTree := &trillian.Tree{
		TreeId: cassTree.TreeID,
		Deleted: cassTree.Deleted,
		DisplayName: cassTree.DisplayName,
		Description: cassTree.Description,
		MaxRootDuration: ptypes.DurationProto(time.Duration(cassTree.MaxRootDurationMillis * int64(time.Millisecond))),
	}
	var err error
	trilTree.CreateTime, err = ptypes.TimestampProto(storage.FromMillisSinceEpoch(cassTree.CreateTimeMillis))
	if err != nil {
		return nil, fmt.Errorf("failed to build tree.CreateTime: %v", err)
	}
	trilTree.UpdateTime, err = ptypes.TimestampProto(storage.FromMillisSinceEpoch(cassTree.UpdateTimeMillis))
	if err != nil {
		return nil, fmt.Errorf("failed to build tree.UpdateTime: %v", err)
	}
	trilTree.DeleteTime, err = ptypes.TimestampProto(storage.FromMillisSinceEpoch(cassTree.DeleteTimeMillis))
	if err != nil {
		return nil, fmt.Errorf("failed to build tree.DeleteTime: %v", err)
	}
	// Duped from sql.go  ...  convert all things!
	if ts, ok := trillian.TreeState_value[cassTree.TreeState]; ok {
		trilTree.TreeState = trillian.TreeState(ts)
	} else {
		return nil, fmt.Errorf("unknown TreeState: %v", cassTree.TreeState)
	}
	if tt, ok := trillian.TreeType_value[cassTree.TreeType]; ok {
		trilTree.TreeType = trillian.TreeType(tt)
	} else {
		return nil, fmt.Errorf("unknown TreeType: %v", cassTree.TreeType)
	}
	if hs, ok := trillian.HashStrategy_value[cassTree.HashStrategy]; ok {
		trilTree.HashStrategy = trillian.HashStrategy(hs)
	} else {
		return nil, fmt.Errorf("unknown HashStrategy: %v", cassTree.HashStrategy)
	}
	if ha, ok := spb.DigitallySigned_HashAlgorithm_value[cassTree.HashAlgorithm]; ok {
		trilTree.HashAlgorithm = spb.DigitallySigned_HashAlgorithm(ha)
	} else {
		return nil, fmt.Errorf("unknown HashAlgorithm: %v", cassTree.HashAlgorithm)
	}
	if sa, ok := spb.DigitallySigned_SignatureAlgorithm_value[cassTree.SignatureAlgorithm]; ok {
		trilTree.SignatureAlgorithm = spb.DigitallySigned_SignatureAlgorithm(sa)
	} else {
		return nil, fmt.Errorf("unknown SignatureAlgorithm: %v", cassTree.SignatureAlgorithm)
	}
	return trilTree, nil
}

func (t *cassAdminTX) GetTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
	glog.Infof("Admin.GetTree: treeID=%d", treeID)
	treesTable := t.ks.Table("trees", &tree{}, gocassa.Keys{
		PartitionKeys: []string{"tree_id"},
  }).WithOptions(gocassa.Options{TableName: "trees"})
	treeResult := &tree{}
	err := treesTable.Where(gocassa.Eq("tree_id", treeID)).ReadOne(treeResult).Run()
	if err != nil {
		return nil, err
	}
	glog.Infof("GetTree: read tree %v", treeResult)
	return cassTreeToTrillianTree(treeResult)
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
	CreateTimeMillis int64 `cql:"create_time_millis"`
	UpdateTimeMillis int64 `cql:"update_time_millis"`
	DeleteTimeMillis int64 `cql:"delete_time_millis"`
	Deleted bool `cql:"deleted"`
	DisplayName string `cql:"display_name"`
	Description string `cql:"description"`
	TreeState string `cql:"tree_state"`
	TreeType string `cql:"tree_type"`
	HashStrategy string `cql:"hash_strategy"`
	HashAlgorithm string `cql:"hash_algorithm"`
	SignatureAlgorithm string `cql:"signature_algorithm"`
	// TODO(phad): Should be `cql:"max_root_duration_millis"`.  GetTree fails with
	// ```can not unmarshal duration into *int64``` - possibly down in gocql?
	MaxRootDurationMillis int64 `cql:"-"`
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
		trilTree, err := cassTreeToTrillianTree(&treeResult)
		if err != nil {
			return nil, err
		}
		result[i] = trilTree
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

func validateStorageSettings(trilTree *trillian.Tree) error {
	// TODO(phad): implement me.
	return nil
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

	if err := storage.ValidateTreeForCreation(ctx, trilTree); err != nil {
		return nil, err
	}
	if err := validateStorageSettings(trilTree); err != nil {
		return nil, err
	}

	id, err := storage.NewTreeID()
	if err != nil {
		return nil, err
	}

	// Use the time truncated-to-millis throughout, as that's what's stored.
	nowMillis := storage.ToMillisSinceEpoch(time.Now())
	now := storage.FromMillisSinceEpoch(nowMillis)

	newTree := proto.Clone(trilTree).(*trillian.Tree)
	newTree.TreeId = id
	newTree.CreateTime, err = ptypes.TimestampProto(now)
	if err != nil {
		return nil, fmt.Errorf("failed to build create time: %v", err)
	}
	newTree.UpdateTime, err = ptypes.TimestampProto(now)
	if err != nil {
		return nil, fmt.Errorf("failed to build update time: %v", err)
	}
	rootDuration, err := ptypes.Duration(newTree.MaxRootDuration)
	if err != nil {
		return nil, fmt.Errorf("could not parse MaxRootDuration: %v", err)
	}

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
		TreeID:  id,
		GroupID: defGroupID,
	}).Add(treesTable.Set(tree{
		TreeID:      id,
		CreateTimeMillis: nowMillis,
		UpdateTimeMillis: nowMillis,
		Deleted: false,
		DisplayName: newTree.DisplayName,
		Description: newTree.Description,
		TreeState: newTree.TreeState.String(),
		TreeType: newTree.TreeType.String(),
		HashStrategy: newTree.HashStrategy.String(),
		HashAlgorithm: newTree.HashAlgorithm.String(),
		SignatureAlgorithm: newTree.SignatureAlgorithm.String(),
		MaxRootDurationMillis: int64(rootDuration/time.Millisecond),
	})).RunLoggedBatchWithContext(ctx); err != nil {
		return nil, err
	}
	return newTree, nil
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
