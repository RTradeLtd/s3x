package temx

import (
	"context"

	pb "github.com/RTradeLtd/TxPB/go"
)

func (x *xObjects) bucketToIPFS(ctx context.Context, bucket *Bucket) (string, error) {
	bucketData, err := bucket.Marshal()
	if err != nil {
		return "", err
	}
	resp, err := x.dagClient.DagPut(ctx, &pb.DagPutRequest{
		Data: bucketData,
	})
	if err != nil {
		return "", err
	}
	return resp.GetHashes()[0], nil
}

func (x *xObjects) bucketFromIPFS(ctx context.Context, name string) (*Bucket, error) {
	hash, err := x.ledgerStore.GetBucketHash(name)
	if err != nil {
		return nil, err
	}
	resp, err := x.dagClient.DagGet(ctx, &pb.DagGetRequest{
		Hash: hash,
	})
	if err != nil {
		return nil, err
	}
	bucket := &Bucket{}
	if err := bucket.Unmarshal(resp.GetRawData()); err != nil {
		return nil, err
	}
	return bucket, nil
}

func (x *xObjects) objectFromBucket(ctx context.Context, bucketName, objectName string) (*Object, error) {
	objectHash, err := x.ledgerStore.GetObjectHashFromBucket(bucketName, objectName)
	if err != nil {
		return nil, err
	}
	return x.objectFromHash(ctx, objectHash)
}

func (x *xObjects) objectFromHash(ctx context.Context, objectHash string) (*Object, error) {
	resp, err := x.dagClient.DagGet(ctx, &pb.DagGetRequest{
		Hash: objectHash,
	})
	if err != nil {
		return nil, err
	}
	object := &Object{}
	if err := object.Unmarshal(resp.GetRawData()); err != nil {
		return nil, err
	}
	return object, nil
}
