package s3x

import (
	"context"
	"errors"
	fmt "fmt"

	minio "github.com/RTradeLtd/s3x/cmd"
)

// ListMultipartUploads lists all multipart uploads.
func (x *xObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, e error) {
	fmt.Println("list multipart uploads")
	return lmi, errors.New("not yet implemented")
}

// NewMultipartUpload upload object in multiple parts
func (x *xObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, o minio.ObjectOptions) (uploadID string, err error) {
	fmt.Println("new multipart upload")
	return uploadID, errors.New("not yet implemented")
}

// PutObjectPart puts a part of object in bucket
func (x *xObjects) PutObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (pi minio.PartInfo, e error) {
	fmt.Println("put object part")
	return pi, errors.New("not yet implemented")
}

// CopyObjectPart creates a part in a multipart upload by copying
// existing object or a part of it.
func (x *xObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string,
	partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	fmt.Println("copy object part")
	return p, errors.New("not yet implemented")
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (x *xObjects) ListObjectParts(ctx context.Context, bucket string, object string, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (lpi minio.ListPartsInfo, e error) {
	fmt.Println("list object parts")
	return lpi, errors.New("not yet implemented")
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (x *xObjects) AbortMultipartUpload(ctx context.Context, bucket string, object string, uploadID string) error {
	fmt.Println("abort multipart upload")
	return errors.New("not yet implemented")
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (x *xObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (oi minio.ObjectInfo, e error) {
	fmt.Println("complete multipart uplaod")
	return oi, errors.New("not yet implemented")

}
