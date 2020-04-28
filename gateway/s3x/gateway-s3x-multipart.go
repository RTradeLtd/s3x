package s3x

import (
	"context"
	"errors"
	fmt "fmt"
	"time"

	proto "github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	unixfs_pb "github.com/ipfs/go-unixfs/pb"
	minio "github.com/minio/minio/cmd"
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
	return pi, x.toMinioErr(
		x.ledgerStore.PutObjectPart(bucket, object, uploadID, pi),
		bucket, object, uploadID)
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
		return oi, x.toMinioErr(err, bucket, object, uploadID)
	}
	m, unlock, err := x.ledgerStore.GetObjectDetails(uploadID)
	if err != nil {
		return oi, x.toMinioErr(err, bucket, object, uploadID)
	}
	defer unlock()
	totalSize := uint64(0)
	links := make([]*ipld.Link, 0, len(uploadedParts))
	blocks := make([]uint64, 0, len(uploadedParts))
	for _, p := range uploadedParts {
		number := int64(p.PartNumber)
		pi, ok := m.ObjectParts[number]
		if !ok {
			return oi, x.toMinioErr(fmt.Errorf("PartNumber %v not found", number), bucket, object, uploadID)
		}
		if pi.ActualSize <= 0 {
			return oi, x.toMinioErr(fmt.Errorf("PartNumber %v reported ActualSize as %v", number, pi.ActualSize), bucket, object, uploadID)
		}
		cid, err := cid.Decode(pi.DataHash)
		if err != nil {
			return oi, x.toMinioErr(fmt.Errorf("PartNumber %v hash is not cid, %v", number, err), bucket, object, uploadID)
		}
		size := uint64(pi.ActualSize)
		totalSize += size
		links = append(links, &ipld.Link{
			Size: size,
			Cid:  cid,
		})
		blocks = append(blocks, size)
	}
	protoNode := &merkledag.ProtoNode{}
	protoNode.SetCidBuilder(merkledag.V1CidPrefix())
	protoNode.SetLinks(links)
	data, err := proto.Marshal(&unixfs_pb.Data{
		Type:       unixfs_pb.Data_File.Enum(),
		Filesize:   &totalSize,
		Blocksizes: blocks,
	})
	if err != nil {
		return oi, x.toMinioErr(err, bucket, object, uploadID)
	}
	protoNode.SetData(data)
	dataHash, err := ipfsSaveProtoNode(ctx, x.dagClient, protoNode)
	if err != nil {
		return oi, x.toMinioErr(err, bucket, object, uploadID)
	}
	loi := m.ObjectInfo
	if loi == nil || len(opts.UserDefined) != 0 {
		noi := newObjectInfo(bucket, object, int(totalSize), opts)
		loi = &noi
	} else {
		loi.Size_ = int64(totalSize)
		loi.ModTime = time.Now().UTC()
	}
	err = x.ledgerStore.PutObject(ctx, bucket, object, &Object{
		DataHash:   dataHash,
		ObjectInfo: *loi,
	})
	if err != nil {
		return oi, x.toMinioErr(err, bucket, object, uploadID)
	}
	return getMinioObjectInfo(loi), x.AbortMultipartUpload(ctx, bucket, object, uploadID)
}
