package s3x

import (
	"context"

	pb "github.com/RTradeLtd/TxPB/v3/go"
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
