package s3x

import (
	"context"

	pb "github.com/RTradeLtd/TxPB/go"
	minio "github.com/RTradeLtd/s3x/cmd"
)

/* Design Notes
---------------

These functions should never call `toMinioErr`, and instead bubble up the erorrs.
Any error parsing to return minio errors should be done in the calling S3 functions.
*/

func (x *xObjects) bucketToIPFS(ctx context.Context, bucket *Bucket) (string, error) {
	bucketData, err := bucket.Marshal()
	if err != nil {
		return "", err
	}
	return x.dagPut(ctx, bucketData)
}

func (x *xObjects) objectToIPFS(ctx context.Context, obj *Object) (string, error) {
	objData, err := obj.Marshal()
	if err != nil {
		return "", err
	}
	return x.dagPut(ctx, objData)
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
	data, err := x.dagGet(ctx, hash)
	bucket := &Bucket{}
	if err := bucket.Unmarshal(data); err != nil {
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
	data, err := x.dagGet(ctx, objectHash)
	if err != nil {
		return nil, err
	}
	object := &Object{}
	if err := object.Unmarshal(data); err != nil {
		return nil, err
	}
	return object, nil
}

func (x *xObjects) getMinioObjectInfo(ctx context.Context, bucketName, objectName string) (minio.ObjectInfo, error) {
	obj, err := x.objectFromBucket(ctx, bucketName, objectName)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	return minio.ObjectInfo{
		Bucket:      obj.GetObjectInfo().Bucket,
		Name:        objectName,
		ETag:        minio.ToS3ETag(obj.GetObjectInfo().Etag),
		Size:        obj.GetObjectInfo().Size_,
		ModTime:     obj.GetObjectInfo().ModTime,
		ContentType: obj.GetObjectInfo().ContentType,
		UserDefined: obj.GetObjectInfo().UserDefined,
	}, nil
}

func (x *xObjects) dagPut(ctx context.Context, data []byte) (string, error) {
	resp, err := x.dagClient.DagPut(ctx, &pb.DagPutRequest{Data: data})
	if err != nil {
		return "", err
	}
	return resp.GetHashes()[0], nil
}

func (x *xObjects) dagGet(ctx context.Context, hash string) ([]byte, error) {
	resp, err := x.dagClient.DagGet(ctx, &pb.DagGetRequest{
		Hash: hash,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetRawData(), nil
}
