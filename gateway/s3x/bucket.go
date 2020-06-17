package s3x

import (
	"context"
	"log"
	"time"

	minio "github.com/minio/minio/cmd"
)

var isTest = false

// MakeBucket creates a new bucket container within TemporalX.
func (x *xObjects) MakeBucketWithLocation(
	ctx context.Context,
	name, location string,
	lockEnabled bool,
) error {
	if lockEnabled {
		// lockEnabled was added in https://github.com/minio/minio/pull/9548/files#diff-ffa5495ade5a5a87da532a15efe2c38b
		// which is also NotImplemented in the s3 gateway as of may 21st, 2020
		return minio.NotImplemented{}
	}

	b := &Bucket{BucketInfo: BucketInfo{
		Location: location,
	}}
	if !isTest { // creates consistent hashes for testing
		b.BucketInfo.Created = time.Now().UTC()
	}
	hash, err := x.ledgerStore.CreateBucket(ctx, name, b)
	if err != nil {
		return x.toMinioErr(err, name, "", "")
	}
	log.Printf("bucket-name: %s\tbucket-hash: %s", name, hash)
	return nil
}

// GetBucketInfo gets bucket metadata..
func (x *xObjects) GetBucketInfo(
	ctx context.Context,
	bucket string,
) (bi minio.BucketInfo, err error) {
	b, err := x.ledgerStore.GetBucketInfo(ctx, bucket)
	if err != nil {
		return bi, x.toMinioErr(err, bucket, "", "")
	}
	return minio.BucketInfo{
		Name: bucket,
		// TODO(bonedaddy): decide what to do here,
		// in the examples of other gateway its a nil time
		// bucket the bucket actually has a created timestamp
		// Created: time.Unix(0, 0),
		Created: b.Created,
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
		//TODO(George): detect context cancelation here (or in GetBucketInfo), as this could be a long running process
		info, err := x.GetBucketInfo(ctx, name)
		if err != nil {
			return nil, err // no need to handle GetBucketInfo parses error accordingly
		}
		infos[i] = info
	}
	return infos, nil
}

// DeleteBucket deletes a bucket on S3
func (x *xObjects) DeleteBucket(ctx context.Context, name string, forceDelete bool) error {
	// TODO(bonedaddy): implement removal call from TemporalX
	return x.toMinioErr(x.ledgerStore.DeleteBucket(name), name, "", "")
}
