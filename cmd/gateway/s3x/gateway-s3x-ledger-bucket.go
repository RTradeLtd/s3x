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
	b, err := ls.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, ErrLedgerBucketDoesNotExist
	}
	err = b.ensureCache(ctx, ls.dag)
	if err != nil {
		return nil, err
	}
	bi := b.Bucket.GetBucketInfo()
	return &bi, nil
}

// getBucket returns a lazy loading LedgerBucketEntry
//
// if err is returned, then the datastore can not be read
// if nil, nil is return, then bucket does not exit
func (ls *ledgerStore) getBucket(bucket string) (*LedgerBucketEntry, error) {
	b, ok := ls.l.Buckets[bucket]
	if !ok {
		bHash, err := ls.ds.Get(dsBucketKey.ChildString(bucket))
		if err != nil {
			if err == datastore.ErrNotFound {
				ls.l.Buckets[bucket] = nil
				return nil, nil
			}
			return nil, err
		}
		b = &LedgerBucketEntry{
			IpfsHash: string(bHash),
		}
		ls.l.Buckets[bucket] = b
	}
	return b, nil
}

//CreateBucket saves a new bucket iff it did not exist
func (ls *ledgerStore) CreateBucket(ctx context.Context, bucket string, b *Bucket) error {
	defer ls.locker.write(bucket)()
	return ls.createBucket(ctx, bucket, b)
}

func (ls *ledgerStore) createBucket(ctx context.Context, bucket string, b *Bucket) error {
	ex, err := ls.bucketExists(bucket)
	if err != nil {
		return err
	}
	if ex {
		return ErrLedgerBucketExists
	}
	if b == nil {
		panic("can not create nil bucket")
	}
	b.BucketInfo.Name = bucket
	return ls.saveBucket(ctx, bucket, b)
}

func (ls *ledgerStore) saveBucket(ctx context.Context, bucket string, b *Bucket) error {
	//set or check if bucket is valid
	if b.BucketInfo.Name == "" {
		b.BucketInfo.Name = bucket
	}
	if b.BucketInfo.Name != bucket {
		return fmt.Errorf("bucket name miss match %v != %v", bucket, b.BucketInfo.Name)
	}

	//save to ipfs and get hash
	bHash, err := ipfsSave(ctx, ls.dag, b)
	if err != nil {
		return err
	}
	if err := ls.ds.Put(dsBucketKey.ChildString(bucket), []byte(bHash)); err != nil {
		return err
	}

	//save hash to ledger
	ls.l.Buckets[bucket] = &LedgerBucketEntry{
		Bucket:   b,
		IpfsHash: bHash,
	}
	return nil
}

func (ls *ledgerStore) bucketExists(bucket string) (bool, error) {
	b, err := ls.getBucket(bucket)
	return b != nil, err
}

// DeleteBucket is used to remove a ledger bucket entry
func (ls *ledgerStore) DeleteBucket(bucket string) error {
	defer ls.locker.write(bucket)()
	ex, err := ls.bucketExists(bucket)
	if err != nil {
		return err
	}
	if !ex {
		return ErrLedgerBucketDoesNotExist
	}
	delete(ls.l.Buckets, bucket)
	return ls.ds.Delete(dsBucketKey.ChildString(bucket))
	//todo: remove from ipfs
}
