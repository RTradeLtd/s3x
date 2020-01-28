package s3x

import (
	"context"
	"fmt"

	pb "github.com/RTradeLtd/TxPB/v3/go"
	"github.com/ipfs/go-datastore"
)

// cacheLocker protects from simultaneous calls to ensureCache for the same bucket,
// which might only hold a read lock otherwise for speed.
var cacheLocker = bucketLocker{}

func (m *LedgerBucketEntry) ensureCache(ctx context.Context, dag pb.NodeAPIClient) error {
	// locking on IpfsHash is the same as locking on bucket name in this context,
	// because it's cannot change without retrieving the old value.
	defer cacheLocker.write(m.IpfsHash)()
	if m.Bucket == nil {
		b, err := ipfsBucket(ctx, dag, m.IpfsHash)
		if err != nil {
			return err
		}
		if m.Bucket != nil {
			panic("ensureCache state changed unexpectedly, this should never happen")
		}
		m.Bucket = b
	}
	return nil
}

//GetBucketInfo returns the BucketInfo in ledger,
//possible errors include ErrLedgerBucketDoesNotExist and dag network errors.
func (ls *ledgerStore) GetBucketInfo(ctx context.Context, bucket string) (*BucketInfo, error) {
	defer ls.locker.read(bucket)()
	b, err := ls.getBucketLoaded(ctx, bucket)
	if err != nil {
		return nil, err
	}
	bi := b.Bucket.GetBucketInfo()
	return &bi, nil
}

//GetBucketHash return the hash of the bucket if the named bucket exist
func (ls *ledgerStore) GetBucketHash(bucket string) (string, error) {
	defer ls.locker.read(bucket)()
	b, err := ls.getBucketRequired(bucket)
	if err != nil {
		return "", err
	}
	return b.IpfsHash, nil
}

// getBucketNilable returns a lazy loading LedgerBucketEntry
//
// if err is returned, then the datastore can not be read
// if nil, nil is return, then bucket does not exit
func (ls *ledgerStore) getBucketNilable(bucket string) (*LedgerBucketEntry, error) {
	ls.mapLocker.Lock()
	b, ok := ls.l.Buckets[bucket]
	ls.mapLocker.Unlock()
	if !ok {
		bHash, err := ls.ds.Get(dsBucketKey.ChildString(bucket))
		if err != nil {
			if err == datastore.ErrNotFound {
				ls.mapLocker.Lock()
				ls.l.Buckets[bucket] = nil
				ls.mapLocker.Unlock()
				return nil, nil
			}
			return nil, err
		}
		//Update bucket only if it's not loaded
		ls.mapLocker.Lock()
		b, ok = ls.l.Buckets[bucket]
		if !ok {
			b = &LedgerBucketEntry{
				IpfsHash: string(bHash),
			}
			ls.l.Buckets[bucket] = b
		}
		ls.mapLocker.Unlock()
	}
	return b, nil
}

// getBucketRequired returns a lazy loading LedgerBucketEntry
//
// if err is returned, then the datastore can not be read,
// or the bucket does not exit
func (ls *ledgerStore) getBucketRequired(bucket string) (*LedgerBucketEntry, error) {
	b, err := ls.getBucketNilable(bucket)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, ErrLedgerBucketDoesNotExist
	}
	return b, nil
}

// getBucketLoaded returns a loaded LedgerBucketEntry
//
// if err is returned, then the datastore can not be read,
// or the bucket does not exit
func (ls *ledgerStore) getBucketLoaded(ctx context.Context, bucket string) (*LedgerBucketEntry, error) {
	b, err := ls.getBucketRequired(bucket)
	if err != nil {
		return nil, err
	}
	if err := b.ensureCache(ctx, ls.dag); err != nil {
		return nil, err
	}
	return b, nil
}

//CreateBucket saves a new bucket iff it did not exist
func (ls *ledgerStore) CreateBucket(ctx context.Context, bucket string, b *Bucket) (string, error) {
	defer ls.locker.write(bucket)()
	lb, err := ls.createBucket(ctx, bucket, b)
	if err != nil {
		return "", err
	}
	return lb.IpfsHash, nil
}

func (ls *ledgerStore) createBucket(ctx context.Context, bucket string, b *Bucket) (*LedgerBucketEntry, error) {
	if b == nil {
		panic("can not create nil bucket")
	}
	ex, err := ls.bucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if ex {
		return nil, ErrLedgerBucketExists
	}
	if b.BucketInfo.Name == "" {
		b.BucketInfo.Name = bucket
	}
	return ls.saveBucket(ctx, bucket, b)
}

func (ls *ledgerStore) saveBucket(ctx context.Context, bucket string, b *Bucket) (*LedgerBucketEntry, error) {
	//check if bucket is valid
	if b.BucketInfo.Name != bucket {
		return nil, fmt.Errorf("bucket name miss match %v != %v", bucket, b.BucketInfo.Name)
	}

	//save to ipfs and get hash
	bHash, err := ipfsSave(ctx, ls.dag, b)
	if err != nil {
		return nil, err
	}
	if err := ls.ds.Put(dsBucketKey.ChildString(bucket), []byte(bHash)); err != nil {
		return nil, err
	}

	//save hash to ledger
	lb := &LedgerBucketEntry{
		Bucket:   b,
		IpfsHash: bHash,
	}
	ls.mapLocker.Lock()
	ls.l.Buckets[bucket] = lb
	ls.mapLocker.Unlock()
	return lb, nil
}

func (ls *ledgerStore) AssertBucketExits(bucket string) error {
	unlock := ls.locker.read(bucket)
	err := ls.assertBucketExits(bucket)
	unlock()
	return err
}

func (ls *ledgerStore) assertBucketExits(bucket string) error {
	ex, err := ls.bucketExists(bucket)
	if err != nil {
		return err
	}
	if !ex {
		return ErrLedgerBucketDoesNotExist
	}
	return nil
}

func (ls *ledgerStore) bucketExists(bucket string) (bool, error) {
	b, err := ls.getBucketNilable(bucket)
	return b != nil, err
}

// DeleteBucket is used to remove a ledger bucket entry
func (ls *ledgerStore) DeleteBucket(bucket string) error {
	defer ls.locker.write(bucket)()
	err := ls.assertBucketExits(bucket)
	if err != nil {
		return err
	}
	ls.mapLocker.Lock()
	delete(ls.l.Buckets, bucket)
	ls.mapLocker.Unlock()
	return ls.ds.Delete(dsBucketKey.ChildString(bucket))
	//todo: remove from ipfs
}
