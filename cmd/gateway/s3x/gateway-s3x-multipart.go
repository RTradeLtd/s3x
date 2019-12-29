package s3x

import (
	"context"
	"errors"
	fmt "fmt"
	"io"
	"time"

	pb "github.com/RTradeLtd/TxPB/go"
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
	o minio.ObjectOptions,
) (uploadID string, err error) {
	if !x.ledgerStore.BucketExists(bucket) {
		return "", x.toMinioErr(ErrLedgerBucketDoesNotExist, bucket, "", "")
	}
	uploadID = ksuid.New().String()
	return uploadID, x.toMinioErr(
		x.ledgerStore.NewMultipartUpload(bucket, object, uploadID),
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
	if !x.ledgerStore.BucketExists(bucket) {
		return pi, x.toMinioErr(ErrLedgerBucketDoesNotExist, bucket, "", "")
	}
	// add the given data to ipfs
	stream, err := x.fileClient.UploadFile(ctx)
	if err != nil {
		return pi, err
	}
	var (
		buf  = make([]byte, 4194294)
		size int
	)
	for {
		n, err := r.Read(buf)
		if err != nil && err == io.EOF {
			if n == 0 {
				break
			}
		} else if err != nil && err != io.EOF {
			return pi, err
		}
		size = size + n
		if err := stream.Send(&pb.UploadRequest{
			Blob: &pb.Blob{Content: buf[:n]},
		}); err != nil {
			return pi, err
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return pi, err
	}
	err = x.ledgerStore.PutObjectPart(bucket, object, resp.GetHash(), uploadID, int64(partID))
	if err != nil {
		return pi, x.toMinioErr(err, bucket, object, uploadID)
	}
	return minio.PartInfo{
		PartNumber:   partID,
		LastModified: time.Now().UTC(),
		Size:         int64(size),
	}, nil
}

// CopyObjectPart creates a part in a multipart upload by copying
// existing object or a part of it.
func (x *xObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string,
	partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	fmt.Println("copy object part")
	return p, errors.New("not yet implemented")
}

// ListObjectParts returns all object parts for specified object in specified bucket
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
	parts, err := x.ledgerStore.GetObjectParts(uploadID)
	if err != nil {
		return lpi, x.toMinioErr(err, bucket, object, uploadID)
	}
	for _, part := range parts {
		lpi.Parts = append(lpi.Parts, minio.PartInfo{
			PartNumber: int(part.GetNumber()),
			ETag:       minio.ToS3ETag(part.GetEtag()),
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
	if !x.ledgerStore.BucketExists(bucket) {
		return oi, x.toMinioErr(ErrLedgerBucketDoesNotExist, bucket, object, uploadID)
	}
	return oi, errors.New("not finished yet")

}
