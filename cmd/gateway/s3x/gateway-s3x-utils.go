package s3x

import (
	"context"

	minio "github.com/RTradeLtd/s3x/cmd"
)

/* Design Notes
---------------

These functions should never call `toMinioErr`, and instead bubble up the errors.
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
