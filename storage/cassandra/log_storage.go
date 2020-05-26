package cassandra

import (
	"context"
	"errors"
	"time"

	"github.com/golang/glog"
	"github.com/google/trillian"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/storage"
	"github.com/monzo/gocassa"
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

func (m *cassLogStorage) Snapshot(context.Context) (storage.ReadOnlyLogTX, error) {
	glog.Infof("cassLogStorage.Snapshot")
	return nil, errors.New("cassLogStorage.Snapshot: not implemented")
}

func (m *cassLogStorage) ReadWriteTransaction(_ context.Context, tree *trillian.Tree, _ storage.LogTXFunc) error {
	glog.Infof("cassLogStorage.ReadWriteTransaction: tree=%v")
	return errors.New("cassLogStorage.ReadWriteTransaction: not implemented")
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
