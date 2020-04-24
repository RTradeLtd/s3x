package s3x

import (
	"context"
	"sort"
	"strings"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"go.uber.org/multierr"
)

/* Design Notes
---------------

Internal functions should never claim or release locks.
Any claiming or releasing of locks should be done in the public setter+getter functions.
The reason for this is so that we can enable easy reuse of internal code.
*/

// Close shuts down the ledger datastore
func (ls *ledgerStore) Close() error {
	var err error
	for _, f := range ls.cleanup {
		err = multierr.Append(err, f())
	}
	return multierr.Append(err, ls.ds.Close())
}

/////////////////////
// GETTER FUNCTINS //
/////////////////////

// GetObjectInfos returns a list of ordered ObjectInfos with given prefix ordered by name
func (ls *ledgerStore) GetObjectInfos(ctx context.Context, bucket, prefix, startsFrom string, max int) ([]ObjectInfo, error) {
	defer ls.locker.read(bucket)()
	b, err := ls.getBucketLoaded(ctx, bucket)
	if err != nil {
		return nil, err
	}
	var names []string
	objs := b.GetBucket().GetObjects()
	for name := range objs {
		if strings.HasPrefix(name, prefix) && strings.Compare(startsFrom, name) <= 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if max > 0 && len(names) > max {
		names = names[:max]
	}
	list := make([]ObjectInfo, 0, len(names))
	for _, name := range names {
		obj, err := ls.object(ctx, bucket, name)
		if err != nil {
			return nil, err
		}
		list = append(list, obj.GetObjectInfo())
	}
	return list, nil
}

// GetObjectHash is used to retrieve the corresponding IPFS CID for an object
func (ls *ledgerStore) GetObjectHash(ctx context.Context, bucket, object string) (string, error) {
	objs, unlock, err := ls.GetObjectHashes(ctx, bucket)
	if err != nil {
		return "", err
	}
	defer unlock()
	h, ok := objs[object]
	if !ok {
		return "", ErrLedgerObjectDoesNotExist
	}
	return h, nil
}

// GetObjectHashes gets a map of object names to object hashes for all objects in a bucket.
// The returned function must be called to release a read lock, iff an error is not returned.
func (ls *ledgerStore) GetObjectHashes(ctx context.Context, bucket string) (map[string]string, func(), error) {
	unlock := ls.locker.read(bucket)
	b, err := ls.getBucketLoaded(ctx, bucket)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	return b.Bucket.Objects, unlock, nil
}

// GetBucketNames is used to get a slice of all bucket names our ledger currently tracks
func (ls *ledgerStore) GetBucketNames() ([]string, error) {
	//this only reads from the datastore, which have it's own synchronization, so no locking is needed.
	rs, err := ls.ds.Query(query.Query{
		Prefix:   dsBucketKey.String(),
		KeysOnly: true,
	})
	if err != nil {
		return nil, err
	}
	names := []string{}
	for r := range rs.Next() {
		names = append(names, datastore.NewKey(r.Key).BaseNamespace())
	}
	return names, nil
}
