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
	b := &Bucket{BucketInfo: BucketInfo{
		Location: location,
		Created:  time.Now().UTC(),
	}}
	err := x.ledgerStore.creatBucket(ctx, name, b)
	if err != nil {
		return x.toMinioErr(err, name, "", "")
	}
	log.Printf("bucket-name: %s\tbucket-hash: %s", name, x.ledgerStore.l.Buckets[name].IpfsHash)
	return nil
}

// GetBucketInfo gets bucket metadata..
func (x *xObjects) GetBucketInfo(
	ctx context.Context,
	name string,
) (bi minio.BucketInfo, err error) {
	defer func() {
		err = x.toMinioErr(err, name, "", "")
	}()
	b, err := x.ledgerStore.getBucket(name)
	if err != nil {
		return
	}
	if b == nil {
		err = ErrLedgerBucketDoesNotExist
		return
	}
	err = b.ensureCache(ctx, x.dagClient)
	if err != nil {
		return
	}
	return minio.BucketInfo{
		Name: name,
		// TODO(bonedaddy): decide what to do here,
		// in the examples of other gateway its a nil time
		// bucket the bucket actually has a created timestamp
		// Created: time.Unix(0, 0),
		Created: b.Bucket.GetBucketInfo().Created,
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
	// TODO(bonedaddy): implement removal call from TemporalX
	return x.toMinioErr(x.ledgerStore.DeleteBucket(name), name, "", "")
}
