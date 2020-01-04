package s3x

import (
	"context"

	pb "github.com/RTradeLtd/TxPB/v3/go"
	minio "github.com/RTradeLtd/s3x/cmd"
)

/* Design Notes
---------------

These functions should never call `toMinioErr`, and instead bubble up the erorrs.
Any error parsing to return minio errors should be done in the calling S3 functions.
*/

// bucketToIPFS takes a bucket and stores it on IPFS
func (x *xObjects) bucketToIPFS(ctx context.Context, bucket *Bucket) (string, error) {
	bucketData, err := bucket.Marshal()
	if err != nil {
		return "", err
	}
	return x.dagPut(ctx, bucketData)
}

// objectToIPFS takes an object and stores it on IPFs
func (x *xObjects) objectToIPFS(ctx context.Context, obj *Object) (string, error) {
	objData, err := obj.Marshal()
	if err != nil {
		return "", err
	}
	return x.dagPut(ctx, objData)
}

// addObjectToBucketandIPFS is used to update a bucket with the ipfs hash, and name of an object that belongs to it.
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

// bucketFromIPFS returns a bucket from IPFS using its name
func (x *xObjects) bucketFromIPFS(ctx context.Context, name string) (bucket *Bucket, err error) {
	hash, err := x.ledgerStore.GetBucketHash(name)
	if err != nil {
		return nil, err
	}
	data, err := x.dagGet(ctx, hash)
	if err != nil {
		return nil, err
	}
	bucket = new(Bucket)
	err = bucket.Unmarshal(data)
	return
}

// objectFromBucket returns an object from IPFS using its name, and the bucket it belongs to
func (x *xObjects) objectFromBucket(ctx context.Context, bucketName, objectName string) (*Object, error) {
	objectHash, err := x.ledgerStore.GetObjectHash(bucketName, objectName)
	if err != nil {
		return nil, err
	}
	return x.objectFromHash(ctx, objectHash)
}

// objectFromHash returns an object from IPFS using its hash
func (x *xObjects) objectFromHash(ctx context.Context, objectHash string) (obj *Object, err error) {
	data, err := x.dagGet(ctx, objectHash)
	if err != nil {
		return nil, err
	}
	obj = new(Object)
	err = obj.Unmarshal(data)
	return
}

// getMinioObjectInfo is used to convert between object info in our protocol buffer format, to a minio object layer info type
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

// dagPut is a helper function to store arbitrary byte slices on IPFS as IPLD objects
func (x *xObjects) dagPut(ctx context.Context, data []byte) (string, error) {
	resp, err := x.dagClient.Dag(ctx, &pb.DagRequest{
		RequestType: pb.DAGREQTYPE_DAG_PUT,
		Data:        data,
	})
	if err != nil {
		return "", err
	}
	return resp.GetHashes()[0], nil
}

// dagGet is a helper function to return byte slices from IPLD objects on IPFS
func (x *xObjects) dagGet(ctx context.Context, hash string) ([]byte, error) {
	resp, err := x.dagClient.Dag(ctx, &pb.DagRequest{
		RequestType: pb.DAGREQTYPE_DAG_GET,
		Hash:        hash,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetRawData(), nil
}
