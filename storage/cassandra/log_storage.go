package cassandra

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/trillian"
	"github.com/google/trillian/merkle/hashers"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/storage/cache"
	"github.com/google/trillian/types"
	"github.com/monzo/gocassa"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	defaultLogStrata = []int{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}
)

// NewLogStorage creates a storage.LogStorage instance for Cassandra.
// It assumes storage.AdminStorage is backed by the same Cassandra database.
func NewLogStorage(ks gocassa.KeySpace, mf monitoring.MetricFactory) storage.LogStorage {
	if mf == nil {
		mf = monitoring.InertMetricFactory{}
	}
	return &cassLogStorage{
		ks:            ks,
		admin:         NewAdminStorage(ks),
		metricFactory: mf,
	}
}

type cassLogStorage struct {
	*cassTreeStorage
	ks            gocassa.KeySpace
	admin         storage.AdminStorage
	metricFactory monitoring.MetricFactory
}

func (m *cassLogStorage) CheckDatabaseAccessible(context.Context) error {
	if m.ks.Name() == "" {
		return errors.New("cassLogStorage.CheckDatabaseAccessible: Cassandra not ready")
	}
	return nil
}

type cassLogTX struct {
	ks gocassa.KeySpace
}

// readOnlyLogTX implements storage.ReadOnlyLogTX
type readOnlyLogTX struct {
	ls *cassLogStorage
}

func (m *cassLogStorage) Snapshot(context.Context) (storage.ReadOnlyLogTX, error) {
	glog.Infof("cassLogStorage.Snapshot")
	return nil, errors.New("cassLogStorage.Snapshot: not implemented")
}

func (m *cassLogStorage) ReadWriteTransaction(ctx context.Context, tree *trillian.Tree, f storage.LogTXFunc) error {
	tx, err := m.beginInternal(ctx, tree)
	if err != nil && err != storage.ErrTreeNeedsInit {
		return err
	}
	defer tx.Close()
	if err := f(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (m *cassLogStorage) beginInternal(ctx context.Context, tree *trillian.Tree) (*logTreeTX, error) {
	hasher, err := hashers.NewLogHasher(tree.HashStrategy)
	if err != nil {
		return nil, err
	}

	stCache := cache.NewLogSubtreeCache(defaultLogStrata, hasher)
	ttx, err := m.beginTreeTx(ctx, tree, hasher.Size(), stCache)
	if err != nil && err != storage.ErrTreeNeedsInit {
		return nil, err
	}

	// TODO(phad): Read Latest SLR from DB.

	ltx := &logTreeTX{
		treeTX: ttx,
		ks:     m.ks,
	}

	ltx.slr, err = ltx.fetchLatestRoot(ctx)
	if err == storage.ErrTreeNeedsInit {
		return ltx, err
	} else if err != nil {
		ttx.Rollback()
		return nil, err
	}
	if err := ltx.root.UnmarshalBinary(ltx.slr.LogRoot); err != nil {
		ttx.Rollback()
		return nil, err
	}

	ltx.treeTX.writeRevision = int64(ltx.root.Revision) + 1
	return ltx, nil
}

func (m *cassLogStorage) AddSequencedLeaves(_ context.Context, tree *trillian.Tree, leaves []*trillian.LogLeaf, _ time.Time) ([]*trillian.QueuedLogLeaf, error) {
	glog.Infof("cassLogStorage.AddSequencedLeaves: %d leaves for tree=%v", len(leaves), tree)
	return nil, errors.New("cassLogStorage.AddSequencedLeaves: not implemented")
}

func (m *cassLogStorage) SnapshotForTree(ctx context.Context, tree *trillian.Tree) (storage.ReadOnlyLogTreeTX, error) {
	glog.Infof("cassLogStorage.SnapshotForTree: tree=%v", tree)
	tx, err := m.beginInternal(ctx, tree)
	if err != nil && err != storage.ErrTreeNeedsInit {
		return nil, err
	}
	return tx, err
}

func (m *cassLogStorage) QueueLeaves(ctx context.Context, tree *trillian.Tree, leaves []*trillian.LogLeaf, queueTimestamp time.Time) ([]*trillian.QueuedLogLeaf, error) {
	glog.Infof("cassLogStorage.QueueLeaves: %d leaves for tree %v", len(leaves), tree)
	tx, err := m.beginInternal(ctx, tree)
	if tx != nil {
		// Ensure we don't leak the transaction if an error occurs below.
		defer tx.Close()
	}
	if err != nil {
		return nil, err
	}
	existing, err := tx.QueueLeaves(ctx, leaves, queueTimestamp)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	ret := make([]*trillian.QueuedLogLeaf, len(leaves))
	for i, e := range existing {
		if e != nil {
			ret[i] = &trillian.QueuedLogLeaf{
				Leaf:   e,
				Status: status.Newf(codes.AlreadyExists, "leaf already exists: %v", e.LeafIdentityHash).Proto(),
			}
			continue
		}
		ret[i] = &trillian.QueuedLogLeaf{Leaf: leaves[i]}
	}
	return ret, nil
}

type logTreeTX struct {
	treeTX
	ks   gocassa.KeySpace
	root types.LogRootV1
	slr  *trillian.SignedLogRoot
}

func (t *logTreeTX) ReadRevision(ctx context.Context) (int64, error) {
	return 0, errors.New("cassLogStorage.logTreeTX.ReadRevision: not implemented")
}

func (t *logTreeTX) WriteRevision(ctx context.Context) (int64, error) {
	return 0, errors.New("cassLogStorage.logTreeTX.WriteRevision: not implemented")
}

func (t *logTreeTX) DequeueLeaves(ctx context.Context, limit int, cutoffTime time.Time) ([]*trillian.LogLeaf, error) {
	return nil, errors.New("cassLogStorage.logTreeTX.DequeueLeaves: not implemented")
}

// sortLeavesForInsert returns a slice containing the passed in leaves sorted
// by LeafIdentityHash, and paired with their original positions.
// QueueLeaves and AddSequencedLeaves use this to make the order that LeafData
// row locks are acquired deterministic and reduce the chance of deadlocks.
func sortLeavesForInsert(leaves []*trillian.LogLeaf) []leafAndPosition {
	ordLeaves := make([]leafAndPosition, len(leaves))
	for i, leaf := range leaves {
		ordLeaves[i] = leafAndPosition{leaf: leaf, idx: i}
	}
	sort.Sort(byLeafIdentityHashWithPosition(ordLeaves))
	return ordLeaves
}

func (t *logTreeTX) QueueLeaves(ctx context.Context, leaves []*trillian.LogLeaf, queueTimestamp time.Time) ([]*trillian.LogLeaf, error) {
	glog.Infof("cassLogStorage.logTreeTX.QueueLeaves: will queue %d leaves.", len(leaves))

	t.treeTX.mu.Lock()
	defer t.treeTX.mu.Unlock()

	// Don't accept batches if any of the leaves are invalid.
	for _, leaf := range leaves {
		if len(leaf.LeafIdentityHash) != t.hashSizeBytes {
			return nil, fmt.Errorf("queued leaf must have a leaf ID hash of length %d", t.hashSizeBytes)
		}
		var err error
		leaf.QueueTimestamp, err = ptypes.TimestampProto(queueTimestamp)
		if err != nil {
			return nil, fmt.Errorf("got invalid queue timestamp: %v", err)
		}
	}
	start := time.Now()

	ordLeaves := sortLeavesForInsert(leaves)
	existingCount := 0
	existingLeaves := make([]*trillian.LogLeaf, len(leaves))

	for _, ol := range ordLeaves {
		i, leaf := ol.idx, ol.leaf

		leafStart := time.Now()
		qTimestamp, err := ptypes.Timestamp(leaf.QueueTimestamp)
		if err != nil {
			return nil, fmt.Errorf("got invalid queue timestamp: %v", err)
		}

		leafDataTable := t.ks.MultimapTable("leaf_data", "tree_id", "leaf_identity_hash", &cassLeafData{})
		leafDataTable = leafDataTable.WithOptions(gocassa.Options{TableName: "leaf_data"})
		err = leafDataTable.Set(cassLeafData{
			TreeID:              t.treeID,
			LeafIdentityHash:    leaf.LeafIdentityHash,
			LeafValue:           leaf.LeafValue,
			ExtraData:           leaf.ExtraData,
			QueueTimestampNanos: uint64(qTimestamp.UnixNano()),
		}).RunWithContext(ctx)

		//		_, err = t.tx.ExecContext(ctx, insertLeafDataSQL, t.treeID, leaf.LeafIdentityHash, leaf.LeafValue, leaf.ExtraData, qTimestamp.UnixNano())
		insertDuration := time.Since(leafStart)
		_ = insertDuration
		if isDuplicateErr(err) {
			// Remember the duplicate leaf, using the requested leaf for now.
			existingLeaves[i] = leaf
			existingCount++
			continue
		}
		if err != nil {
			glog.Warningf("Error inserting %d into LeafData: %s", i, err)
			return nil, err
		}

		// Create the work queue entry
		args := []interface{}{
			t.treeID,
			leaf.LeafIdentityHash,
			leaf.MerkleLeafHash,
		}
		queueTimestamp, err := ptypes.Timestamp(leaf.QueueTimestamp)
		if err != nil {
			return nil, fmt.Errorf("got invalid queue timestamp: %v", err)
		}
		args = append(args, queueArgs(t.treeID, leaf.LeafIdentityHash, queueTimestamp)...)

		// TODO(phad): insert unsequenced entry into C* here.

		// _, err = t.tx.ExecContext(
		// 	ctx,
		// 	insertUnsequencedEntrySQL,
		// 	args...,
		// )
		if err != nil {
			glog.Warningf("Error inserting into Unsequenced: %s", err)
			return nil, err
		}
		leafDuration := time.Since(leafStart)
		_ = leafDuration
	}
	insertDuration := time.Since(start)
	_ = insertDuration

	if existingCount == 0 {
		return existingLeaves, nil
	}

	// For existing leaves, we need to retrieve the contents.  First collate the desired LeafIdentityHash values.
	var toRetrieve [][]byte
	for _, existing := range existingLeaves {
		if existing != nil {
			toRetrieve = append(toRetrieve, existing.LeafIdentityHash)
		}
	}
	results, err := t.getLeafDataByIdentityHash(ctx, toRetrieve)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve existing leaves: %v", err)
	}
	if len(results) != len(toRetrieve) {
		return nil, fmt.Errorf("failed to retrieve all existing leaves: got %d, want %d", len(results), len(toRetrieve))
	}
	// Replace the requested leaves with the actual leaves.
	for i, requested := range existingLeaves {
		if requested == nil {
			continue
		}
		found := false
		for _, result := range results {
			if bytes.Equal(result.LeafIdentityHash, requested.LeafIdentityHash) {
				existingLeaves[i] = result
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("failed to find existing leaf for hash %x", requested.LeafIdentityHash)
		}
	}
	return existingLeaves, nil
}

func (t *logTreeTX) AddSequencedLeaves(ctx context.Context, leaves []*trillian.LogLeaf, timestamp time.Time) ([]*trillian.QueuedLogLeaf, error) {
	return nil, errors.New("cassLogStorage.logTreeTX.AddSequencedLeaves: not implemented")
}

func (t *logTreeTX) GetSequencedLeafCount(ctx context.Context) (int64, error) {
	return 0, errors.New("cassLogStorage.logTreeTX.GetSequencedLeafCount: not implemented")
}

func (t *logTreeTX) GetLeavesByIndex(ctx context.Context, leaves []int64) ([]*trillian.LogLeaf, error) {
	return nil, errors.New("cassLogStorage.logTreeTX.GetLeavesByIndex: not implemented")
}

func (t *logTreeTX) GetLeavesByRange(ctx context.Context, start, count int64) ([]*trillian.LogLeaf, error) {
	return nil, errors.New("cassLogStorage.logTreeTX.GetLeavesByRange: not implemented")
}

func (t *logTreeTX) GetLeavesByHash(ctx context.Context, leafHashes [][]byte, orderBySequence bool) ([]*trillian.LogLeaf, error) {
	return nil, errors.New("cassLogStorage.logTreeTX.GetLeavesByHash: not implemented")
}

// getLeafDataByIdentityHash retrieves leaf data by LeafIdentityHash, returned
// as a slice of LogLeaf objects for convenience.  However, note that the
// returned LogLeaf objects will not have a valid MerkleLeafHash, LeafIndex, or IntegrateTimestamp.
func (t *logTreeTX) getLeafDataByIdentityHash(ctx context.Context, leafHashes [][]byte) ([]*trillian.LogLeaf, error) {
	return nil, errors.New("cassLogStorage.logTreeTX.getLeafDataByIdentityHash: not implemented")

}

func (t *logTreeTX) LatestSignedLogRoot(ctx context.Context) (*trillian.SignedLogRoot, error) {
	glog.Infof("cassandra.logTreeTX.LatestSignedLogRoot(): slr=%v", t.slr)
	return t.slr, nil
}

// fetchLatestRoot reads the latest SignedLogRoot from the DB and returns it.
func (t *logTreeTX) fetchLatestRoot(ctx context.Context) (*trillian.SignedLogRoot, error) {
	treesTable := t.ks.Table("trees", &cassTree{}, gocassa.Keys{
		PartitionKeys: []string{"tree_id"},
	}).WithOptions(gocassa.Options{TableName: "trees"})

	tree := cassTree{}
	if err := treesTable.Where(gocassa.Eq("tree_id", t.treeID)).ReadOne(&tree).Run(); err != nil {
		return nil, err
	}
	if len(tree.CurrentSignedLogRootJSON) == 0 {
		return nil, storage.ErrTreeNeedsInit
	}

	var logRoot types.LogRootV1
	json.Unmarshal(tree.CurrentSignedLogRootJSON, &logRoot)
	newRoot, _ := logRoot.MarshalBinary()
	return &trillian.SignedLogRoot{
		KeyHint:          types.SerializeKeyHint(t.treeID),
		LogRoot:          newRoot,
		LogRootSignature: tree.RootSignature,
	}, nil
}

func (t *logTreeTX) StoreSignedLogRoot(ctx context.Context, root *trillian.SignedLogRoot) error {
	glog.Infof("cassandra.logTreeTX.StoreSignedLogRoot(): root=%v", root)

	var logRoot types.LogRootV1
	if err := logRoot.UnmarshalBinary(root.LogRoot); err != nil {
		glog.Warningf("Failed to parse log root: %x %v", root.LogRoot, err)
		return err
	}
	if len(logRoot.Metadata) != 0 {
		return fmt.Errorf("unimplemented: cassandra storage does not support LogRoot.metadata")
	}
	// Serialize the SignedLogRoot within the trees record as a JSON blob.
	data, _ := json.Marshal(logRoot)

	treesTable := t.ks.Table("trees", &cassTree{}, gocassa.Keys{
		PartitionKeys: []string{"tree_id"},
	}).WithOptions(gocassa.Options{TableName: "trees"})

	treeHeadsTable := t.ks.Table("tree_heads", &cassTreeHead{}, gocassa.Keys{
		PartitionKeys: []string{"tree_id", "revision", "timestamp_nanos"},
	}).WithOptions(gocassa.Options{TableName: "tree_heads"})

	if err := treesTable.Where(gocassa.Eq("tree_id", t.treeID)).Update(map[string]interface{}{
		"current_slr_json": data,
		"root_signature":   root.LogRootSignature,
	}).Add(treeHeadsTable.Set(cassTreeHead{
		TreeID:         t.treeID,         // PK
		Revision:       logRoot.Revision, // PK
		TimestampNanos: logRoot.TimestampNanos,
		Size:           logRoot.TreeSize,
		RootHash:       logRoot.RootHash,
		RootSignature:  root.LogRootSignature,
	})).RunLoggedBatchWithContext(ctx); err != nil {
		glog.Warningf("Failed to store signed root: %s", err)
		return err
	}
	return nil
}

func (t *logTreeTX) UpdateSequencedLeaves(ctx context.Context, leaves []*trillian.LogLeaf) error {
	return errors.New("cassLogStorage.logTreeTX.UpdateSequencedLeaves: not implemented")
}

// leafAndPosition records original position before sort.
type leafAndPosition struct {
	leaf *trillian.LogLeaf
	idx  int
}

// byLeafIdentityHashWithPosition allows sorting (as above), but where we need
// to remember the original position
type byLeafIdentityHashWithPosition []leafAndPosition

func (l byLeafIdentityHashWithPosition) Len() int {
	return len(l)
}
func (l byLeafIdentityHashWithPosition) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
func (l byLeafIdentityHashWithPosition) Less(i, j int) bool {
	return bytes.Compare(l[i].leaf.LeafIdentityHash, l[j].leaf.LeafIdentityHash) == -1
}

func isDuplicateErr(err error) bool {
	// switch err := err.(type) {
	// case *mysql.MySQLError:
	// 	return err.Number == errNumDuplicate
	// default:
	// 	return false
	// }
	return false
}

func queueArgs(_ int64, _ []byte, queueTimestamp time.Time) []interface{} {
	return []interface{}{queueTimestamp.UnixNano()}
}
