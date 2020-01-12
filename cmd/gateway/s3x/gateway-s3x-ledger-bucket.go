package s3x

import (
	"context"

	pb "github.com/RTradeLtd/TxPB/v3/go"
	"github.com/ipfs/go-datastore"
)

func (m *LedgerBucketEntry) ensureCache(ctx context.Context, dag pb.NodeAPIClient) error {
	if m.Bucket == nil {
		b, err := ipfsBucket(ctx, dag, m.IpfsHash)
		if err != nil {
			return err
		}
		if m.Bucket != nil {
			panic("ensureCache state changed unexpectedly")
		}
		m.Bucket = b
	}
	return nil
}

func (m *LedgerBucketEntry) size(ctx context.Context, dag pb.NodeAPIClient) (int, error) {
	err := m.ensureCache(ctx, dag)
	if err != nil {
		return 0, err
	}
	return len(m.Bucket.Objects), nil
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

func (ls *ledgerStore) creatBucket(ctx context.Context, bucket string, b *Bucket) error {
	ex, err := ls.BucketExists(bucket)
	if err != nil {
		return err
	}
	if ex {
		return ErrLedgerBucketExists
	}
	if b == nil {
		panic("can not create nil bucket")
	}
	return ls.saveBucket(ctx, bucket, b)
}

func (ls *ledgerStore) saveBucket(ctx context.Context, bucket string, b *Bucket) error {
	bHash, err := ipfsSave(ctx, ls.dag, b)
	if err != nil {
		return err
	}
	if err := ls.ds.Put(dsBucketKey.ChildString(bucket), []byte(bHash)); err != nil {
		return err
	}
	ls.l.Buckets[bucket] = &LedgerBucketEntry{
		Bucket:   b,
		IpfsHash: bHash,
	}
	return nil
}

func (ls *ledgerStore) BucketExists(bucket string) (bool, error) {
	b, err := ls.getBucket(bucket)
	return b != nil, err
}

// DeleteBucket is used to remove a ledger bucket entry
func (ls *ledgerStore) DeleteBucket(bucket string) error {
	ex, err := ls.BucketExists(bucket)
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
