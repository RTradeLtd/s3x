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

// GetObjectInfos returns a list of ordered lists of objects and folders with the given filters
func (ls *ledgerStore) GetObjectInfos(ctx context.Context, bucket, prefix, startsFrom, delimiter string, max int) ([]ObjectInfo, []string, string, error) {
	defer ls.locker.read(bucket)()
	b, err := ls.getBucketLoaded(ctx, bucket)
	if err != nil {
		return nil, nil, "", err
	}
	var names []string
	objs := b.GetBucket().GetObjects()
	for name := range objs {
		if strings.HasPrefix(name, prefix) && strings.Compare(startsFrom, name) <= 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	count := 0 //number of objects counted, up to max
	objects := make([]ObjectInfo, 0)
	folders := make([]string, 0)
	lastFolder := ""
	for _, name := range names {
		if max > 0 && count >= max {
			return objects, folders, name, nil
		}
		sub := name[len(prefix):]
		idx := -1
		if delimiter != "" {
			idx = strings.Index(sub, delimiter)
		}
		if idx < 0 {
			obj, err := ls.object(ctx, bucket, name)
			if err != nil {
				return nil, nil, "", err
			}
			objects = append(objects, obj.GetObjectInfo())
			count++
		} else {
			folder := prefix + sub[:idx] + delimiter
			if folder != lastFolder {
				folders = append(folders, folder)
				lastFolder = folder
				count++
			}
		}
	}
	return objects, folders, "", nil
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
