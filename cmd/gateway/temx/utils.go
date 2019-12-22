package temx

import (
	"context"
	"time"

	pb "github.com/RTradeLtd/TxPB/go"
	minio "github.com/RTradeLtd/s3x/cmd"
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

func (x *xObjects) objectToIPFS(ctx context.Context, obj *Object) (string, error) {
	objData, err := obj.Marshal()
	if err != nil {
		return "", err
	}
	resp, err := x.dagClient.DagPut(ctx, &pb.DagPutRequest{
		Data: objData,
	})
	if err != nil {
		return "", err
	}
	return resp.GetHashes()[0], nil
}

func (x *xObjects) addObjectToBucketAndIPFS(ctx context.Context, objectName, objectHash, bucketName string) (string, error) {
	bucket, err := x.bucketFromIPFS(ctx, bucketName)
	if err != nil {
		return "", err
	}
	if bucket.Objects == nil {
		bucket.Objects = make(map[string]string)
	}
	bucket.Objects[objectName] = objectHash
	return x.bucketToIPFS(ctx, bucket)
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
	objectHash, err := x.ledgerStore.GetObjectHash(bucketName, objectName)
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

func (x *xObjects) getMinioObjectInfo(ctx context.Context, bucketName, objectName string) (minio.ObjectInfo, error) {
	obj, err := x.objectFromBucket(ctx, bucketName, objectName)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	modTime, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", obj.GetObjectInfo().GetModTime())
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	return minio.ObjectInfo{
		Bucket:      obj.GetObjectInfo().GetBucket(),
		Name:        objectName,
		ETag:        minio.ToS3ETag(obj.GetObjectInfo().GetEtag()),
		Size:        obj.GetObjectInfo().GetSize_(),
		ModTime:     modTime,
		ContentType: obj.GetObjectInfo().GetContentType(),
		UserDefined: obj.GetObjectInfo().GetUserDefined(),
	}, nil
}

// toMinioErr converts gRPC or ledger errors into compatible minio errors
// or if no error is present return nil
func (x *xObjects) toMinioErr(err error, bucket, object string) error {
	switch err {
	case ErrLedgerBucketDoesNotExist:
		err = minio.BucketNotFound{Bucket: bucket}
	case ErrLedgerObjectDoesNotExist:
		err = minio.ObjectNotFound{Bucket: bucket, Object: object}
	case ErrLedgerBucketExists:
		err = minio.BucketAlreadyExists{Bucket: bucket}
	case nil:
		return nil
	}
	return err
}
