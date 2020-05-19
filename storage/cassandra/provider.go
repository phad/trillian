package cassandra

import (
	"sync"

	"github.com/golang/glog"
	"github.com/google/trillian/monitoring"
	"github.com/google/trillian/storage"

	// TODO(phad): Load cassandra driver
)

var (
cassOnce            sync.Once
cassOnceErr         error
cassStorageInstance *cassProvider
)

func init() {
	if err := storage.RegisterProvider("cassandra", newCassProvider); err != nil {
		glog.Fatalf("Failed to register storage provider 'cassandra': %v", err)
	}
}

type cassProvider struct {
  // TODO(phad): DB connection
	mf monitoring.MetricFactory
}

func newCassProvider(mf monitoring.MetricFactory) (storage.Provider, error) {
	cassOnce.Do(func() {
    // TODO(phad): connect to Cassandra
		cassStorageInstance = &cassProvider{
			mf: mf,
		}
	})
	if cassOnceErr != nil {
		return nil, cassOnceErr
	}
	return cassStorageInstance, nil
}

func (s *cassProvider) LogStorage() storage.LogStorage {
	glog.Warningf("Support for the Cassandra log is experimental.  Please use at your own risk!!!")
	// TODO(phad): return NewLogStorage(...)
	return NewLogStorage(s.mf)
}

func (s *cassProvider) MapStorage() storage.MapStorage {
  // TODO(phad): return NewMapStorage(...)
	panic("Not Implemented")
}

func (s *cassProvider) AdminStorage() storage.AdminStorage {
	glog.Warningf("Support for the Cassandra log is experimental.  Please use at your own risk!!!")
	return NewAdminStorage()
}

func (s *cassProvider) Close() error {
  // TODO(phad): close the underlying Cassandra DB connection.
	return nil
}
