package cassandra

import (
  "context"
  "errors"

	"github.com/google/trillian"
	"github.com/google/trillian/storage/cache"
  "github.com/google/trillian/storage/tree"
)

type cassTreeStorage struct {
}

func (c *cassTreeStorage) beginTreeTx(context.Context, *trillian.Tree, /*hashSizeBytes*/ int, *cache.SubtreeCache) (treeTX, error) {
  return treeTX{}, errors.New("cassandra.beginTreeTx not implemented <-- DO THIS NEXT")
}

type treeTX struct {
}

func (t *treeTX) Commit(ctx context.Context) error {
  return errors.New("cassandra.treeTx.Commit not implemented")
}

func (t *treeTX) Rollback() error {
  return errors.New("cassandra.treeTx.Rollback not implemented")
}

func (t *treeTX) Close() error {
  return errors.New("cassandra.treeTx.Close not implemented")
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
