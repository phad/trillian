package cassandra

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/google/trillian"
	kpb "github.com/google/trillian/crypto/keyspb"
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

func cassTreeToTrillianTree(cTree *cassTree) (*trillian.Tree, error) {
	trTree := &trillian.Tree{
		TreeId:          cTree.TreeID,
		Deleted:         cTree.Deleted,
		DisplayName:     cTree.DisplayName,
		Description:     cTree.Description,
		MaxRootDuration: ptypes.DurationProto(time.Duration(cTree.MaxRootDurationMillis * int64(time.Millisecond))),
	}
	var err error
	trTree.CreateTime, err = ptypes.TimestampProto(storage.FromMillisSinceEpoch(cTree.CreateTimeMillis))
	if err != nil {
		return nil, fmt.Errorf("failed to build tree.CreateTime: %v", err)
	}
	trTree.UpdateTime, err = ptypes.TimestampProto(storage.FromMillisSinceEpoch(cTree.UpdateTimeMillis))
	if err != nil {
		return nil, fmt.Errorf("failed to build tree.UpdateTime: %v", err)
	}
	trTree.DeleteTime, err = ptypes.TimestampProto(storage.FromMillisSinceEpoch(cTree.DeleteTimeMillis))
	if err != nil {
		return nil, fmt.Errorf("failed to build tree.DeleteTime: %v", err)
	}
	// Duped from sql.go  ...  convert all things!
	if ts, ok := trillian.TreeState_value[cTree.TreeState]; ok {
		trTree.TreeState = trillian.TreeState(ts)
	} else {
		return nil, fmt.Errorf("unknown TreeState: %v", cTree.TreeState)
	}
	if tt, ok := trillian.TreeType_value[cTree.TreeType]; ok {
		trTree.TreeType = trillian.TreeType(tt)
	} else {
		return nil, fmt.Errorf("unknown TreeType: %v", cTree.TreeType)
	}
	if hs, ok := trillian.HashStrategy_value[cTree.HashStrategy]; ok {
		trTree.HashStrategy = trillian.HashStrategy(hs)
	} else {
		return nil, fmt.Errorf("unknown HashStrategy: %v", cTree.HashStrategy)
	}
	if ha, ok := spb.DigitallySigned_HashAlgorithm_value[cTree.HashAlgorithm]; ok {
		trTree.HashAlgorithm = spb.DigitallySigned_HashAlgorithm(ha)
	} else {
		return nil, fmt.Errorf("unknown HashAlgorithm: %v", cTree.HashAlgorithm)
	}
	if sa, ok := spb.DigitallySigned_SignatureAlgorithm_value[cTree.SignatureAlgorithm]; ok {
		trTree.SignatureAlgorithm = spb.DigitallySigned_SignatureAlgorithm(sa)
	} else {
		return nil, fmt.Errorf("unknown SignatureAlgorithm: %v", cTree.SignatureAlgorithm)
	}
	trTree.PrivateKey = &any.Any{}
	if err := proto.Unmarshal(cTree.PrivateKey, trTree.PrivateKey); err != nil {
		return nil, fmt.Errorf("could not unmarshal PrivateKey: %v", err)
	}
	trTree.PublicKey = &kpb.PublicKey{Der: cTree.PublicKey}
	return trTree, nil
}

func (t *cassAdminTX) GetTree(ctx context.Context, treeID int64) (*trillian.Tree, error) {
	glog.Infof("Admin.GetTree: treeID=%d", treeID)
	treesTable := t.ks.Table("trees", &cassTree{}, gocassa.Keys{
		PartitionKeys: []string{"tree_id"},
	}).WithOptions(gocassa.Options{TableName: "trees"})
	treeResult := &cassTree{}
	err := treesTable.Where(gocassa.Eq("tree_id", treeID)).ReadOne(treeResult).Run()
	if err != nil {
		return nil, err
	}
	glog.Infof("GetTree: read tree %v", treeResult)
	return cassTreeToTrillianTree(treeResult)
}

func (t *cassAdminTX) ListTrees(ctx context.Context, includeDeleted bool) ([]*trillian.Tree, error) {
	glog.Infof("Admin.ListTrees: includeDeleted=%t", includeDeleted)
	treeIDs, err := t.ListTreeIDs(ctx, includeDeleted)
	if err != nil {
		return nil, err
	}
	result := make([]*trillian.Tree, len(treeIDs))
	treesTable := t.ks.Table("trees", &cassTree{}, gocassa.Keys{
		PartitionKeys: []string{"tree_id"},
	})
	treesTable = treesTable.WithOptions(gocassa.Options{TableName: "trees"})
	treeResult := cassTree{}
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
	groupsByNameTable := t.ks.Table("groups_by_name", &cassGroup{}, gocassa.Keys{
		PartitionKeys: []string{"group_name"},
	})
	groupsByNameTable = groupsByNameTable.WithOptions(gocassa.Options{TableName: "groups_by_name"})
	groupResult := cassGroup{}
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

	treesByGroupIDTable := t.ks.MultimapTable("trees_by_group_id", "group_id", "tree_id", &cassTreeGroup{})
	treesByGroupIDTable = treesByGroupIDTable.WithOptions(gocassa.Options{TableName: "trees_by_group_id"})
	treeGroupsResult := []cassTreeGroup{}
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
	privateKey, err := proto.Marshal(newTree.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("could not marshal PrivateKey: %v", err)
	}

	defGroupID, err := t.defaultGroupID(ctx)
	if err != nil {
		return nil, err
	}

	treesByGroupIDTable := t.ks.MultimapTable("trees_by_group_id", "group_id", "tree_id", &cassTreeGroup{})
	treesByGroupIDTable = treesByGroupIDTable.WithOptions(gocassa.Options{TableName: "trees_by_group_id"})
	treesTable := t.ks.Table("trees", &cassTree{}, gocassa.Keys{
		PartitionKeys: []string{"tree_id"},
	})
	treesTable = treesTable.WithOptions(gocassa.Options{TableName: "trees"})

	if err := treesByGroupIDTable.Set(cassTreeGroup{
		TreeID:  id,
		GroupID: defGroupID,
	}).Add(treesTable.Set(cassTree{
		TreeID:                id,
		CreateTimeMillis:      nowMillis,
		UpdateTimeMillis:      nowMillis,
		Deleted:               false,
		DisplayName:           newTree.DisplayName,
		Description:           newTree.Description,
		TreeState:             newTree.TreeState.String(),
		TreeType:              newTree.TreeType.String(),
		HashStrategy:          newTree.HashStrategy.String(),
		HashAlgorithm:         newTree.HashAlgorithm.String(),
		SignatureAlgorithm:    newTree.SignatureAlgorithm.String(),
		MaxRootDurationMillis: int64(rootDuration / time.Millisecond),
		PrivateKey:            privateKey,
		PublicKey:             newTree.PublicKey.GetDer(),
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
