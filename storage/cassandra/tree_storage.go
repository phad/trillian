package cassandra

import (
	"context"
	"errors"
	"sync"

	"github.com/golang/glog"
	"github.com/google/trillian"
	"github.com/google/trillian/storage/cache"
	"github.com/google/trillian/storage/tree"
)

type cassTreeStorage struct {
}

func (c *cassTreeStorage) beginTreeTx(_ context.Context, tree *trillian.Tree, hashSizeBytes int, _ *cache.SubtreeCache) (treeTX, error) {
	return treeTX{
		mu:            &sync.Mutex{},
		treeID:        tree.TreeId,
		hashSizeBytes: hashSizeBytes,
	}, nil
}

type treeTX struct {
	// mu ensures that ks can only be used for one query at a time.
	mu            *sync.Mutex
	treeID        int64
	writeRevision int64
	hashSizeBytes int
}

func (t *treeTX) Commit(ctx context.Context) error {
	glog.Infof("cassandra.treeTx.Commit: no-op")
	return nil
}

func (t *treeTX) Rollback() error {
	return errors.New("cassandra.treeTx.Rollback not implemented")
}

func (t *treeTX) Close() error {
	glog.Infof("cassandra.treeTx.Close: no-op")
	return nil
}

func (t *treeTX) GetMerkleNodes(ctx context.Context, treeRevision int64, nodeIDs []tree.NodeID) ([]tree.Node, error) {
	return nil, errors.New("cassandra.treeTx.GetMerkleNodes not implemented")
}

func (t *treeTX) SetMerkleNodes(ctx context.Context, nodes []tree.Node) error {
	return errors.New("cassandra.treeTx.SetMerkleNodes not implemented")
}

func (t *treeTX) IsOpen() bool {
	panic("cassandra.treeTx.Commit not implemented")
	return false
}
