package s3x

import (
	"context"
	"errors"

	"github.com/minio/minio/pkg/bucket/policy"
)

// SetBucketPolicy sets policy on bucket
func (x *xObjects) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	return errors.New("not yet implemented")
}

// GetBucketPolicy will get policy on bucket
func (x *xObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return nil, errors.New("not yet implemented")
}

// DeleteBucketPolicy deletes all policies on bucket
func (x *xObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return errors.New("not yet implemented")
}
