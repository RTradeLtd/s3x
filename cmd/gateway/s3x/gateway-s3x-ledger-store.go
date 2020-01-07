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

func (ls *ledgerStore) BucketExists(bucket string) bool {
	return ls.getBucket(bucket) != nil
}

// DeleteBucket is used to remove a ledger bucket entry
func (ls *ledgerStore) DeleteBucket(name string) error {
	if !ls.BucketExists(name) {
		return ErrLedgerBucketDoesNotExist
	}
	delete(ls.l.Buckets, name)
	return nil //todo: save to ipfs
}

func (ls *ledgerStore) getBucket(bucket string) *LedgerBucketEntry {
	return ls.l.Buckets[bucket]
}

func (ls *ledgerStore) object(ctx context.Context, bucket, object string) (*Object, error) {
	objectHash, err := ls.GetObjectHash(ctx, bucket, object)
	if err != nil {
		return nil, err
	}
	return ipfsObject(ctx, ls.dag, objectHash)
}

func (ls *ledgerStore) objectData(ctx context.Context, bucket, object string) ([]byte, error) {
	obj, err := ls.object(ctx, bucket, object)
	if err != nil {
		return nil, err
	}
	return ipfsBytes(ctx, ls.dag, obj.GetDataHash())
}

// RemoveObject is used to remove a ledger object entry from a ledger bucket entry
func (ls *ledgerStore) RemoveObject(ctx context.Context, dag pb.NodeAPIClient, bucket, object string) error {
	b := ls.getBucket(bucket)
	if b == nil {
		return ErrLedgerBucketDoesNotExist
	}
	err := b.ensureCache(ctx, dag)
	if err != nil {
		return err
	}
	delete(b.Bucket.Objects, object)
	return nil //todo: save on ipfs
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
func ipfsBytes(ctx context.Context, dag pb.NodeAPIClient, h string) ([]byte, error) {
	resp, err := dag.Dag(ctx, &pb.DagRequest{
		RequestType: pb.DAGREQTYPE_DAG_GET,
		Hash:        h,
	})
	return resp.GetRawData(), err
}

type unmarshaller interface {
	Unmarshal(data []byte) error
}

// ipfsUnmarshal unmarshalls any data structure from IPFS using its hash
func ipfsUnmarshal(ctx context.Context, dag pb.NodeAPIClient, h string, u unmarshaller) error {
	data, err := ipfsBytes(ctx, dag, h)
	if err != nil {
		return err
	}
	return u.Unmarshal(data)
}

// ipfsObject returns an object from IPFS using its hash
func ipfsObject(ctx context.Context, dag pb.NodeAPIClient, h string) (*Object, error) {
	obj := &Object{}
	if err := ipfsUnmarshal(ctx, dag, h, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// ipfsBucket returns a bucket from IPFS using its hash
func ipfsBucket(ctx context.Context, dag pb.NodeAPIClient, h string) (*Bucket, error) {
	b := &Bucket{}
	if err := ipfsUnmarshal(ctx, dag, h, b); err != nil {
		return nil, err
	}
	return b, nil
}
