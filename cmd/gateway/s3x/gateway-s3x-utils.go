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

// getMinioObjectInfo is used to convert between object info in our protocol buffer format, to a minio object layer info type
func (x *xObjects) getMinioObjectInfo(ctx context.Context, bucketName, objectName string) (minio.ObjectInfo, error) {
	obj, err := x.ledgerStore.object(ctx, bucketName, objectName)
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
