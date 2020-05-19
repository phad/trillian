package cassandra

import (
	"context"
	"errors"
	"time"

	"github.com/google/trillian"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/storage"
)

// NewLogStorage creates a storage.LogStorage instance for Cassandra.
// It assumes storage.AdminStorage is backed by the same Cassandra database.
func NewLogStorage(mf monitoring.MetricFactory) storage.LogStorage {
	if mf == nil {
		mf = monitoring.InertMetricFactory{}
	}
	return &cassLogStorage{
		admin:         NewAdminStorage(),
		metricFactory: mf,
	}
}

type cassLogStorage struct {
	admin         storage.AdminStorage
	metricFactory monitoring.MetricFactory
}

func (m *cassLogStorage) CheckDatabaseAccessible(context.Context) error {
  return errors.New("cassLogStorage.CheckDatabaseAccessible: not implemented")
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
