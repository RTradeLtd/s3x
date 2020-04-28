package s3x

import (
	minio "github.com/minio/minio/cmd"
)

/* Design Notes
---------------

These functions should never call `toMinioErr`, and instead bubble up the errors.
Any error parsing to return minio errors should be done in the calling S3 functions.
*/

// getMinioObjectInfo is used to convert between object info in our protocol buffer format, to a minio object layer info type
func getMinioObjectInfo(o *ObjectInfo) minio.ObjectInfo {
	if o == nil {
		return minio.ObjectInfo{}
	}
	return minio.ObjectInfo{
		Bucket:      o.Bucket,
		Name:        o.Name,
		ETag:        minio.ToS3ETag(o.Etag),
		Size:        o.Size_,
		ModTime:     o.ModTime,
		ContentType: o.ContentType,
		UserDefined: o.UserDefined,
	}
}
