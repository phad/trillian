package cassandra

import (
	"context"
	"errors"
	"time"

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
	return nil, errors.New("cassLogStorage.Snapshot: not implemented")
}

func (m *cassLogStorage) ReadWriteTransaction(context.Context, *trillian.Tree, storage.LogTXFunc) error {
	return errors.New("cassLogStorage.ReadWriteTransaction: not implemented")
}

func (m *cassLogStorage) AddSequencedLeaves(context.Context, *trillian.Tree, []*trillian.LogLeaf, time.Time) ([]*trillian.QueuedLogLeaf, error) {
	return nil, errors.New("cassLogStorage.AddSequencedLeaves: not implemented")
}

func (m *cassLogStorage) SnapshotForTree(ctx context.Context, tree *trillian.Tree) (storage.ReadOnlyLogTreeTX, error) {
	return nil, errors.New("cassLogStorage.SnapshotForTree: not implemented")
}

func (m *cassLogStorage) QueueLeaves(context.Context, *trillian.Tree, []*trillian.LogLeaf, time.Time) ([]*trillian.QueuedLogLeaf, error) {
	return nil, errors.New("cassLogStorage.QueueLeaves: not implemented")
}
