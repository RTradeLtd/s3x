package s3x

import (
	"context"
	"sync"

	pb "github.com/RTradeLtd/TxPB/v3/go"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
)

/* Design Notes
---------------

Internal functions should never claim or release locks.
Any claiming or releasing of locks should be done in the public setter+getter functions.
The reason for this is so that we can enable easy reuse of internal code.
*/

var (
	dsKey       = datastore.NewKey("ledgerstatekey")
	dsPrefix    = datastore.NewKey("ledgerRoot")
	dsBucketKey = datastore.NewKey("b")
)

// ledgerStore is an internal bookkeeper that
// maps buckets to ipfs cids and keeps a local cache of object names to hashes
//
// Bucket root hashes are saved in the provided data store.
// Object hashes are saved in ipfs and cached in memory,
// Object data is saved in ipfs.
type ledgerStore struct {
	sync.RWMutex //to be changed to per bucket name, once datastore saves each bucket separatory
	ds           datastore.Batching
	dag          pb.NodeAPIClient //to be used as direct access to ipfs to optimise algorithm
	l            *Ledger          //a cache of the values in datastore and ipfs (todo: remove?)

	bucketObjectHashes map[string]map[string]string //a cache of ipfs object hashes by bucket and object name
}

func newLedgerStore(ds datastore.Batching, dag pb.NodeAPIClient) (*ledgerStore, error) {
	ls := &ledgerStore{
		ds:  namespace.Wrap(ds, dsPrefix),
		dag: dag,
	}
	var err error
	ls.l, err = ls.getLedger()
	if err != nil {
		return nil, err
	}
	return ls, err
}

func (ls *ledgerStore) getBucket(ctx context.Context, bucket string) {

}

func (ls *ledgerStore) object(ctx context.Context, bucket, object string) (*Object, error) {
	objectHash, err := ls.GetObjectHash(bucket, object)
	if err != nil {
		return nil, err
	}
	return ls.ipfsObject(ctx, objectHash)
}

func (ls *ledgerStore) objectData(ctx context.Context, bucket, object string) ([]byte, error) {
	obj, err := ls.object(ctx, bucket, object)
	if err != nil {
		return nil, err
	}
	return ls.ipfsBytes(ctx, obj.GetDataHash())
}

// putObject saves an object into the given bucket
func (ls *ledgerStore) putObject(ctx context.Context, bucket, object, objHash string) error {
	return nil
	/*b := ls.l.GetBuckets()[bucket]
	if b.Objects == nil {
		b.Objects = make(map[string]string)
	}
	b.Objects[object] = objHash
	return x.bucketToIPFS(ctx, bucket)*/
}

// ipfsBytes returns data from IPFS using its hash
func (ls *ledgerStore) ipfsBytes(ctx context.Context, h string) ([]byte, error) {
	resp, err := ls.dag.Dag(ctx, &pb.DagRequest{
		RequestType: pb.DAGREQTYPE_DAG_GET,
		Hash:        h,
	})
	return resp.GetRawData(), err
}

type unmarshaller interface {
	Unmarshal(data []byte) error
}

// ipfsUnmarshal unmarshalls any data structure from IPFS using its hash
func (ls *ledgerStore) ipfsUnmarshal(ctx context.Context, h string, u unmarshaller) error {
	data, err := ls.ipfsBytes(ctx, h)
	if err != nil {
		return err
	}
	return u.Unmarshal(data)
}

// ipfsObject returns an object from IPFS using its hash
func (ls *ledgerStore) ipfsObject(ctx context.Context, h string) (*Object, error) {
	obj := &Object{}
	if err := ls.ipfsUnmarshal(ctx, h, obj); err != nil {
		return nil, err
	}
	return obj, nil
}
