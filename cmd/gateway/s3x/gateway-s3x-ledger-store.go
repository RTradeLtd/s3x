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
	locker    bucketLocker //a lock to protect buckets from concurrent access
	ds        datastore.Batching
	dag       pb.NodeAPIClient //to be used as direct access to ipfs to optimize algorithm
	l         *Ledger          //a cache of the values in datastore and ipfs
	mapLocker sync.Mutex       //a lock to protect maps in l from concurrent access
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
	b, err := ls.getBucketLoaded(ctx, bucket)
	if err != nil {
		return nil, err
	}
	objs := b.GetBucket().Objects
	if objs == nil {
		return nil, ErrLedgerObjectDoesNotExist
	}
	h, ok := b.GetBucket().Objects[object]
	if !ok {
		return nil, ErrLedgerObjectDoesNotExist
	}
	return ipfsObject(ctx, ls.dag, h)
}

//ObjectInfo returns the ObjectInfo of the object.
func (ls *ledgerStore) ObjectInfo(ctx context.Context, bucket, object string) (*ObjectInfo, error) {
	obj, err := ls.object(ctx, bucket, object)
	if err != nil {
		return nil, err
	}
	return &obj.ObjectInfo, nil
}

func (ls *ledgerStore) ObjectData(ctx context.Context, bucket, object string) ([]byte, error) {
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
	b, err := ls.getBucketLoaded(ctx, bucket)
	if err != nil {
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

//PutObject saves an object by hash into the given bucket
func (ls *ledgerStore) PutObject(ctx context.Context, bucket, object string, obj *Object) error {
	defer ls.locker.write(bucket)()
	return ls.putObject(ctx, bucket, object, obj)
}

//putObject saves an object by hash into the given bucket
func (ls *ledgerStore) putObject(ctx context.Context, bucket, object string, obj *Object) error {
	oHash, err := ipfsSave(ctx, ls.dag, obj)
	if err != nil {
		return err
	}
	return ls.putObjectHash(ctx, bucket, object, oHash)
}

// putObjectHash saves an object by hash into the given bucket
func (ls *ledgerStore) putObjectHash(ctx context.Context, bucket, object, objHash string) error {
	b, err := ls.getBucketLoaded(ctx, bucket)
	if err != nil {
		return err
	}
	if b.Bucket.Objects == nil {
		b.Bucket.Objects = make(map[string]string)
	}
	b.Bucket.Objects[object] = objHash
	return ls.saveBucket(ctx, bucket, b.Bucket)
}
