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
	dag          pb.NodeAPIClient //to be used as direct access to ipfs to optimize algorithm
	l            *Ledger          //a cache of the values in datastore and ipfs
}

func newLedgerStore(ds datastore.Batching, dag pb.NodeAPIClient) (*ledgerStore, error) {
	ls := &ledgerStore{
		ds:  namespace.Wrap(ds, dsPrefix),
		dag: dag,
		l: &Ledger{
			Buckets:          make(map[string]*LedgerBucketEntry),
			MultipartUploads: make(map[string]MultipartUpload),
		},
	}
	return ls, nil
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
func (ls *ledgerStore) removeObject(ctx context.Context, bucket, object string) error {
	missing, err := ls.removeObjects(ctx, bucket, object)
	if err != nil {
		return err
	}
	if len(missing) != 0 {
		return ErrLedgerObjectDoesNotExist
	}
	return nil
	//todo: gc on ipfs
}

// removeObjects efficiently remove many objects, returns a list of objects that did not exist.
func (ls *ledgerStore) removeObjects(ctx context.Context, bucket string, objects ...string) ([]string, error) {
	b, err := ls.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, ErrLedgerBucketDoesNotExist
	}
	if err := b.ensureCache(ctx, ls.dag); err != nil {
		return nil, err
	}
	if b.Bucket.Objects == nil {
		return objects, nil
	}

	missing := []string{}
	for _, o := range objects {
		_, ok := b.Bucket.Objects[o]
		if !ok {
			missing = append(missing, o)
			continue
		}
		delete(b.Bucket.Objects, o)
	}
	return missing, ls.saveBucket(ctx, bucket, b.Bucket)
	//todo: gc on ipfs
}

//putObject2 saves an object by hash into the given bucket
func (ls *ledgerStore) putObject(ctx context.Context, bucket, object string, obj *Object) error {
	oHash, err := ipfsSave(ctx, ls.dag, obj)
	if err != nil {
		return nil
	}
	return ls.putObjectHash(ctx, bucket, object, oHash)
}

// putObject saves an object by hash into the given bucket
func (ls *ledgerStore) putObjectHash(ctx context.Context, bucket, object, objHash string) error {
	b, err := ls.getBucket(bucket)
	if err != nil {
		return err
	}
	if b == nil {
		return ErrLedgerBucketDoesNotExist
	}
	if err := b.ensureCache(ctx, ls.dag); err != nil {
		return err
	}
	if b.Bucket.Objects == nil {
		b.Bucket.Objects = make(map[string]string)
	}
	b.Bucket.Objects[object] = objHash
	return ls.saveBucket(ctx, bucket, b.Bucket)
}
