package cassandra

import (
	"context"
	"errors"
	"time"

	"github.com/golang/glog"
	"github.com/google/trillian"
	"github.com/google/trillian/merkle/hashers"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/storage"
	"github.com/google/trillian/storage/cache"
	"github.com/monzo/gocassa"
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

func (m *cassLogStorage) beginInternal(ctx context.Context, tree *trillian.Tree) (storage.LogTreeTX, error) {
	hasher, err := hashers.NewLogHasher(tree.HashStrategy)
	if err != nil {
		return nil, err
	}

	stCache := cache.NewLogSubtreeCache(defaultLogStrata, hasher)
	ttx, err := m.beginTreeTx(ctx, tree, hasher.Size(), stCache)
	if err != nil && err != storage.ErrTreeNeedsInit {
		return nil, err
	}

	ltx := &logTreeTX{
		treeTX: ttx,
	}

	// TODO: lots of stuff omitted here from postgres storage.
	return ltx, nil
}

func (m *cassLogStorage) AddSequencedLeaves(_ context.Context, tree *trillian.Tree, leaves []*trillian.LogLeaf, _ time.Time) ([]*trillian.QueuedLogLeaf, error) {
	glog.Infof("cassLogStorage.AddSequencedLeaves: %d leaves for tree=%v", len(leaves), tree)
	return nil, errors.New("cassLogStorage.AddSequencedLeaves: not implemented")
}

func (m *cassLogStorage) SnapshotForTree(ctx context.Context, tree *trillian.Tree) (storage.ReadOnlyLogTreeTX, error) {
	glog.Infof("cassLogStorage.SnapshotForTree: tree=%v", tree)
	return nil, errors.New("cassLogStorage.SnapshotForTree: not implemented")
}

func (m *cassLogStorage) QueueLeaves(_ context.Context, tree *trillian.Tree, leaves []*trillian.LogLeaf, _ time.Time) ([]*trillian.QueuedLogLeaf, error) {
	glog.Infof("cassLogStorage.QueueLeaves: %d leaves for tree %v", len(leaves), tree)
	return nil, errors.New("cassLogStorage.QueueLeaves: not implemented")
}

type logTreeTX struct {
	treeTX
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

func (t *logTreeTX) QueueLeaves(ctx context.Context, leaves []*trillian.LogLeaf, queueTimestamp time.Time) ([]*trillian.LogLeaf, error) {
	return nil, errors.New("cassLogStorage.logTreeTX.QueueLeaves: not implemented")
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

func (t *logTreeTX) LatestSignedLogRoot(ctx context.Context) (*trillian.SignedLogRoot, error) {
	return nil, errors.New("cassLogStorage.logTreeTX.LatestSignedLogRoot: not implemented")
}

func (t *logTreeTX) StoreSignedLogRoot(ctx context.Context, root *trillian.SignedLogRoot) error {
	return errors.New("cassLogStorage.logTreeTX.StoreSignedLogRoot: not implemented")
}

func (t *logTreeTX) UpdateSequencedLeaves(ctx context.Context, leaves []*trillian.LogLeaf) error {
	return errors.New("cassLogStorage.logTreeTX.UpdateSequencedLeaves: not implemented")
}
