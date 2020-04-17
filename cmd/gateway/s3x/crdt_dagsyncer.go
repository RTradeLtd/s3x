package s3x

import (
	"context"

	pb "github.com/RTradeLtd/TxPB/v3/go"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
	"go.uber.org/multierr"
)

//crdtDAGSyncer implements crdt.DAGSyncer using a remote DAGService and a local datastore to account for HasBlock
type crdtDAGSyncer struct {
	dag ipld.DAGService
	ds  datastore.Batching
}

//newCrdtDAGSyncer creates a crdt.DAGSyncer using a NodeAPIClient and local datastore
func newCrdtDAGSyncer(client pb.NodeAPIClient, ds datastore.Batching) *crdtDAGSyncer {
	return &crdtDAGSyncer{
		dag: pb.NewDAGService(client),
		ds:  ds,
	}
}

// Get retrieves nodes by CID. Depending on the NodeGetter
// implementation, this may involve fetching the Node from a remote
// machine; consider setting a deadline in the context.
func (d *crdtDAGSyncer) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	n, err := d.dag.Get(ctx, c)
	return n, d.setBlock(c, err)
}

// GetMany returns a channel of NodeOptions given a set of CIDs.
func (d *crdtDAGSyncer) GetMany(ctx context.Context, cs []cid.Cid) <-chan *ipld.NodeOption {
	out := make(chan *ipld.NodeOption, len(cs))
	go func() {
		for _, c := range cs {
			n, err := d.Get(ctx, c)
			out <- &ipld.NodeOption{
				Node: n,
				Err:  err,
			}
		}
		close(out)
	}()
	return out
}

// Add adds a node to this DAG.
func (d *crdtDAGSyncer) Add(ctx context.Context, n ipld.Node) error {
	return d.AddMany(ctx, []ipld.Node{n})
}

// AddMany adds many nodes to this DAG.
//
// Consider using the Batch NodeAdder (`NewBatch`) if you make
// extensive use of this function.
func (d *crdtDAGSyncer) AddMany(ctx context.Context, ns []ipld.Node) error {
	if err := d.dag.AddMany(ctx, ns); err != nil {
		return err
	}
	for _, n := range ns {
		if err := d.setBlock(n.Cid()); err != nil {
			return err
		}
	}
	return nil
}

// Remove removes a node from this DAG.
//
// Remove returns no error if the requested node is not present in this DAG.
func (d *crdtDAGSyncer) Remove(ctx context.Context, c cid.Cid) error {
	return d.RemoveMany(ctx, []cid.Cid{c})
}

// RemoveMany removes many nodes from this DAG.
//
// It returns success even if the nodes were not present in the DAG.
func (d *crdtDAGSyncer) RemoveMany(ctx context.Context, cs []cid.Cid) error {
	for _, c := range cs {
		if err := d.ds.Delete(datastore.NewKey(c.KeyString())); err != nil {
			return err
		}
	}
	return d.dag.RemoveMany(ctx, cs)
}

// HasBlock returns true if the block is locally available (therefore, it
// is considered processed).
func (d *crdtDAGSyncer) HasBlock(c cid.Cid) (bool, error) {
	return d.ds.Has(datastore.NewKey(c.KeyString()))
}

//setBlock saves this block as true for HasBlock, the optional input error is returned with
//functionality bypassed to pipe errors through.
func (d *crdtDAGSyncer) setBlock(c cid.Cid, errs ...error) error {
	if err := multierr.Combine(errs...); err != nil {
		return err
	}
	return d.ds.Put(datastore.NewKey(c.KeyString()), nil)
}
