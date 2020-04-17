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
	dsBucketKey = datastore.NewKey("b") //bucket name to ipfsHash of LedgerBucketEntry
	dsPartKey   = datastore.NewKey("p") //part ID to MultipartUpload
)

// ledgerStore is an internal bookkeeper that
// maps buckets to ipfs cids and keeps a local cache of object names to hashes
//
// Bucket root hashes are saved in the provided data store.
// Object hashes are saved in ipfs and cached in memory,
// Object data is saved in ipfs.
type ledgerStore struct {
	ds  datastore.Batching
	dag pb.NodeAPIClient //to be used as direct access to ipfs to optimize algorithm
	l   *Ledger          //a cache of the values in datastore and ipfs

	locker     bucketLocker //a locker to protect buckets from concurrent access (per bucket)
	plocker    bucketLocker //a locker to protect MultipartUploads from concurrent access (per upload ID)
	mapLocker  sync.Mutex   //a lock to protect the l.Buckets map from concurrent access
	pmapLocker sync.Mutex   //a lock to protect the l.MultipartUploads map from concurrent access

	cleanup []func() error //a list of functions to call before we close the backing database.
}

func newLedgerStore(ds datastore.Batching, dag pb.NodeAPIClient) (*ledgerStore, error) {
	ls := &ledgerStore{
		ds:  namespace.Wrap(ds, dsPrefix),
		dag: dag,
		l: &Ledger{
			Buckets:          make(map[string]*LedgerBucketEntry),
			MultipartUploads: make(map[string]*MultipartUpload),
		},
	}
	return ls, nil
}

func (ls *ledgerStore) getObjectHash(ctx context.Context, bucket, object string) (string, error) {
	b, err := ls.getBucketLoaded(ctx, bucket)
	if err != nil {
		return "", err
	}
	objs := b.GetBucket().Objects
	if objs == nil {
		return "", ErrLedgerObjectDoesNotExist
	}
	h, ok := objs[object]
	if !ok {
		return "", ErrLedgerObjectDoesNotExist
	}
	return h, nil
}

func (ls *ledgerStore) object(ctx context.Context, bucket, object string) (*Object, error) {
	h, err := ls.getObjectHash(ctx, bucket, object)
	if err != nil {
		return nil, err
	}
	return ipfsObject(ctx, ls.dag, h)
}

//ObjectInfo returns the ObjectInfo of the object.
func (ls *ledgerStore) ObjectInfo(ctx context.Context, bucket, object string) (*ObjectInfo, error) {
	defer ls.locker.read(bucket)()
	obj, err := ls.object(ctx, bucket, object)
	if err != nil {
		return nil, err
	}
	return &obj.ObjectInfo, nil
}

func (ls *ledgerStore) GetObjectDataHash(ctx context.Context, bucket, object string) (string, int64, error) {
	defer ls.locker.read(bucket)()
	obj, err := ls.object(ctx, bucket, object)
	if err != nil {
		return "", 0, err
	}
	return obj.GetDataHash(), obj.ObjectInfo.GetSize_(), nil
}

func (ls *ledgerStore) ObjectData(ctx context.Context, bucket, object string) ([]byte, error) {
	defer ls.locker.read(bucket)()
	obj, err := ls.object(ctx, bucket, object)
	if err != nil {
		return nil, err
	}
	return ipfsBytes(ctx, ls.dag, obj.GetDataHash())
}

func (ls *ledgerStore) RemoveObject(ctx context.Context, bucket, object string) error {
	defer ls.locker.write(bucket)()
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

// RemoveObjects efficiently remove many objects, returns a list of objects that did not exist.
func (ls *ledgerStore) RemoveObjects(ctx context.Context, bucket string, objects ...string) ([]string, error) {
	unlock := ls.locker.write(bucket)
	missing, err := ls.removeObjects(ctx, bucket, objects...)
	unlock()
	return missing, err
}

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
	_, err = ls.saveBucket(ctx, bucket, b.Bucket)
	return missing, err
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
	_, err = ls.saveBucket(ctx, bucket, b.Bucket)
	return err
}
