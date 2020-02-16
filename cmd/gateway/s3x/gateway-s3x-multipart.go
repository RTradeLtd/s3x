package s3x

import (
	"context"
	"errors"
	fmt "fmt"
	"time"

	minio "github.com/RTradeLtd/s3x/cmd"
	"github.com/segmentio/ksuid"
)

// ListMultipartUploads lists all multipart uploads.
func (x *xObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, e error) {
	fmt.Println("list multipart uploads")
	return lmi, errors.New("not yet implemented")
}

// NewMultipartUpload upload object in multiple parts
func (x *xObjects) NewMultipartUpload(
	ctx context.Context,
	bucket, object string,
	opts minio.ObjectOptions,
) (uploadID string, err error) {
	uploadID = ksuid.New().String()
	info := newObjectInfo(bucket, object, 0, opts)
	return uploadID, x.toMinioErr(
		x.ledgerStore.NewMultipartUpload(uploadID, &info),
		bucket, object, uploadID,
	)
}

// PutObjectPart puts a part of object in bucket
func (x *xObjects) PutObjectPart(
	ctx context.Context,
	bucket, object, uploadID string,
	partID int,
	r *minio.PutObjReader,
	opts minio.ObjectOptions,
) (pi minio.PartInfo, e error) {
	err := x.ledgerStore.AssertBucketExits(bucket)
	if err != nil {
		return pi, x.toMinioErr(err, bucket, "", "")
	}
	hash, size, err := ipfsFileUpload(ctx, x.fileClient, r)
	if err != nil {
		return pi, x.toMinioErr(err, bucket, object, uploadID)
	}
	pi = minio.PartInfo{
		PartNumber:   partID,
		LastModified: time.Now().UTC(),
		ETag:         hash,
		Size:         int64(size),
		ActualSize:   int64(size),
	}
	err = x.ledgerStore.PutObjectPart(bucket, object, uploadID, pi)
	if err != nil {
		return pi, x.toMinioErr(err, bucket, object, uploadID)
	}
	return pi, nil
}

// CopyObjectPart creates a part in a multipart upload by copying
// existing object or a part of it.
func (x *xObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string,
	partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	fmt.Println("copy object part")
	return p, errors.New("not yet implemented")
}

// ListObjectParts returns all object parts for specified object in specified bucket
// TODO: paginate using partNumberMarker and maxParts
func (x *xObjects) ListObjectParts(
	ctx context.Context,
	bucket, object, uploadID string,
	partNumberMarker, maxParts int,
	opts minio.ObjectOptions,
) (lpi minio.ListPartsInfo, e error) {
	lpi = minio.ListPartsInfo{
		Bucket:           bucket,
		Object:           object,
		UploadID:         uploadID,
		MaxParts:         maxParts,
		PartNumberMarker: partNumberMarker,
	}
	m, unlock, err := x.ledgerStore.GetObjectDetails(uploadID)
	defer unlock()
	if err != nil {
		return lpi, x.toMinioErr(err, bucket, object, uploadID)
	}
	if m.GetObjectInfo().GetBucket() != bucket ||
		m.GetObjectInfo().GetName() != object {
		return lpi, x.toMinioErr(ErrInvalidUploadID, bucket, object, uploadID)
	}

	for _, part := range m.ObjectParts {
		lpi.Parts = append(lpi.Parts, minio.PartInfo{
			PartNumber: int(part.GetNumber()),
			ETag:       minio.ToS3ETag(part.GetDataHash()),
			Size:       part.GetSize_(),
		})
	}

	return lpi, nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (x *xObjects) AbortMultipartUpload(
	ctx context.Context,
	bucket, object, uploadID string,
) error {
	// TODO(bonedaddy): remove the corresponding objects from ipfs
	return x.toMinioErr(
		x.ledgerStore.AbortMultipartUpload(bucket, uploadID),
		bucket,
		object,
		uploadID,
	)
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object.
// uploadID is interchangeable with multipart id
func (x *xObjects) CompleteMultipartUpload(
	ctx context.Context,
	bucket, object, uploadID string,
	uploadedParts []minio.CompletePart,
	opts minio.ObjectOptions,
) (oi minio.ObjectInfo, e error) {
	err := x.ledgerStore.AssertBucketExits(bucket)
	if err != nil {
		return oi, x.toMinioErr(err, bucket, "", "")
	}
	return oi, errors.New("not finished yet")

}
