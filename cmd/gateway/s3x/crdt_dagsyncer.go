package s3x

import (
	"context"

	pb "github.com/RTradeLtd/TxPB/v3/go"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

//crdtDAGSyncer implements crdt.DAGSyncer using a pb.NodeAPIClient and a datastore
type crdtDAGSyncer struct {
	client pb.NodeAPIClient
	ds     datastore.Batching
}

// Get retrieves nodes by CID. Depending on the NodeGetter
// implementation, this may involve fetching the Node from a remote
// machine; consider setting a deadline in the context.
func (d *crdtDAGSyncer) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	resp, err := d.client.Dag(ctx, &pb.DagRequest{
		RequestType: pb.DAGREQTYPE_DAG_GET,
		Hash:        c.String(),
	})
	if err != nil {
		return nil, err
	}
	block := blocks.NewBlock(resp.RawData)
	if block.Cid() != c {
		return nil, errors.New("unexpected data received from node server")
	}
	n, err := ipld.Decode(block)
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
	cs := make([]string, 0, len(ns))
	for _, n := range ns {
		var format string
		var data []byte
		switch typed := n.(type) {
		default:
			return errors.Errorf("Can not add type: %T using dag client", n)
		case *merkledag.ProtoNode:
			format = "protobuf"
			data = typed.RawData()
		}
		r, err := d.client.Dag(ctx, &pb.DagRequest{
			RequestType:         pb.DAGREQTYPE_DAG_PUT,
			Data:                data,
			SerializationFormat: format,
		})
		if err != nil {
			return err
		}
		if len(r.Hashes) != 1 {
			return errors.New("unexpected number of hashes returned")
		}
		if r.Hashes[0] != n.Cid().String() {
			hcid, err := cid.Decode(r.Hashes[0])
			if err != nil {
				return errors.WithMessage(err, "error decoding returned cid")
			}
			return errors.Errorf("unexpected Cid returned, got %s, prefix %v; want %s, prefix %v",
				r.Hashes[0], hcid.Prefix(), n.Cid().String(), n.Cid().Prefix())
		}
		if err := d.setBlock(n.Cid()); err != nil {
			return err
		}
		cs = append(cs, n.Cid().String())
	}
	_, err := d.client.Persist(ctx, &pb.PersistRequest{Cids: cs}) //Question: should persist be requested before add?
	if err != nil {
		return err
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
	return nil //TODO: remove from d.client
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
