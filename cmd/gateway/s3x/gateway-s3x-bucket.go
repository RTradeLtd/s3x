package s3x

import (
	"context"
	"log"
	"time"

	minio "github.com/RTradeLtd/s3x/cmd"
)

// MakeBucket creates a new bucket container within TemporalX.
func (x *xObjects) MakeBucketWithLocation(
	ctx context.Context,
	name, location string,
) error {
	// check to see whether or not the bucket already exists
	if x.ledgerStore.BucketExists(name) {
		return x.toMinioErr(ErrLedgerBucketExists, name, "", "")
	}
	// create the bucket
	hash, err := x.bucketToIPFS(ctx, &Bucket{
		BucketInfo: BucketInfo{
			Name:     name,
			Location: location,
			Created:  time.Now().UTC(),
		},
	})
	if err != nil {
		return x.toMinioErr(err, name, "", "")
	}
	log.Printf("bucket-name: %s\tbucket-hash: %s", name, hash)
	//  update internal ledger state and return
	return x.toMinioErr(x.ledgerStore.NewBucket(name, hash), name, "", "")
}

// GetBucketInfo gets bucket metadata..
func (x *xObjects) GetBucketInfo(
	ctx context.Context,
	name string,
) (bi minio.BucketInfo, err error) {
	// check to see whether or not the bucket exists
	if !x.ledgerStore.BucketExists(name) {
		return bi, x.toMinioErr(ErrLedgerBucketDoesNotExist, name, "", "")
	}
	bucket, err := x.bucketFromIPFS(ctx, name)
	if err != nil {
		return bi, x.toMinioErr(err, name, "", "")
	}
	return minio.BucketInfo{
		Name: bucket.GetBucketInfo().Name,
		// TODO(bonedaddy): decide what to do here,
		// in the examples of other gateway its a nil time
		// bucket the bucket actually has a created timestamp
		// Created: time.Unix(0, 0),
		Created: bucket.GetBucketInfo().Created,
	}, nil
}

// ListBuckets lists all S3 buckets
func (x *xObjects) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	// TODO(bonedaddy): decide if we should handle a minio error here
	names, err := x.ledgerStore.GetBucketNames()
	if err != nil {
		return nil, err
	}
	var infos = make([]minio.BucketInfo, len(names))
	for i, name := range names {
		info, err := x.GetBucketInfo(ctx, name)
		if err != nil {
			return nil, err // no need to handle GetBucketInfo parses error accordingly
		}
		infos[i] = info
	}
	return infos, nil
}

// DeleteBucket deletes a bucket on S3
func (x *xObjects) DeleteBucket(ctx context.Context, name string) error {
	// check to see whether or not the bucket exists
	if !x.ledgerStore.BucketExists(name) {
		return x.toMinioErr(ErrLedgerBucketDoesNotExist, name, "", "")
	}
	// prevent deleting non-empty bucktes
	if err := x.ledgerStore.IsEmptyBucket(name); err != nil {
		return x.toMinioErr(err, name, "", "")
	}
	// TODO(bonedaddy): implement removal call from TemporalX
	return x.toMinioErr(x.ledgerStore.DeleteBucket(name), name, "", "")
}
