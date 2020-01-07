package s3x

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	minio "github.com/RTradeLtd/s3x/cmd"
)

// ListObjects lists all blobs in S3 bucket filtered by prefix
func (x *xObjects) ListObjects(
	ctx context.Context,
	bucket, prefix, marker, delimiter string,
	maxKeys int,
) (loi minio.ListObjectsInfo, e error) {
	// TODO(bonedaddy): implement complex search
	if !x.ledgerStore.BucketExists(bucket) {
		return loi, x.toMinioErr(ErrLedgerBucketDoesNotExist, bucket, "", "")
	}
	objHashes, err := x.ledgerStore.GetObjectHashes(bucket)
	if err != nil {
		return loi, x.toMinioErr(err, bucket, "", "")
	}
	loi.Objects = make([]minio.ObjectInfo, len(objHashes))
	var count int
	for name := range objHashes {
		info, err := x.getMinioObjectInfo(ctx, bucket, name)
		if err != nil {
			return loi, x.toMinioErr(err, bucket, name, "")
		}
		loi.Objects[count] = info
		count++
	}
	// TODO(bonedaddy): consider if we should use the following helper func
	// return minio.FromMinioClientListBucketResult(bucket, result), nil
	return loi, nil
}

// ListObjectsV2 lists all objects in B2 bucket filtered by prefix, returns upto max 1000 entries at a time.
func (x *xObjects) ListObjectsV2(
	ctx context.Context,
	bucket, prefix, continuationToken, delimiter string,
	maxKeys int,
	fetchOwner bool,
	startAfter string,
) (loi minio.ListObjectsV2Info, err error) {
	if !x.ledgerStore.BucketExists(bucket) {
		return loi, x.toMinioErr(ErrLedgerBucketDoesNotExist, bucket, "", "")
	}
	objHashes, err := x.ledgerStore.GetObjectHashes(bucket)
	if err != nil {
		return loi, x.toMinioErr(err, bucket, "", "")
	}

	if len(objHashes) >= 1000 {
		loi.Objects = make([]minio.ObjectInfo, 1000)
	} else {
		loi.Objects = make([]minio.ObjectInfo, len(objHashes))
	}
	var count int
	for name := range objHashes {
		info, err := x.getMinioObjectInfo(ctx, bucket, name)
		if err != nil {
			return loi, x.toMinioErr(err, bucket, name, "")
		}
		loi.Objects[count] = info
		count++
	}
	return loi, nil
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (x *xObjects) GetObjectNInfo(
	ctx context.Context,
	bucket, object string,
	rs *minio.HTTPRangeSpec,
	h http.Header,
	lockType minio.LockType,
	opts minio.ObjectOptions,
) (gr *minio.GetObjectReader, err error) {
	objinfo, err := x.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return gr, err // the error from this is already properly converted
	}
	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objinfo.Size)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	go func() {
		err := x.GetObject(ctx, bucket, object, startOffset, length, pw, objinfo.ETag, opts)
		pw.CloseWithError(err)
	}()
	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objinfo, opts.CheckCopyPrecondFn, pipeCloser)
}

// GetObject reads an object from TemporalX. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (x *xObjects) GetObject(
	ctx context.Context,
	bucket, object string,
	startOffset, length int64,
	writer io.Writer,
	etag string,
	opts minio.ObjectOptions,
) error {
	objData, err := x.ledgerStore.objectData(ctx, bucket, object)
	if err != nil {
		return x.toMinioErr(err, bucket, object, "")
	}
	end := startOffset + length
	objSize := int64(len(objData))
	if objSize < end {
		return minio.InvalidRange{
			OffsetBegin:  startOffset,
			OffsetEnd:    end,
			ResourceSize: objSize,
		}
	}
	reader := bytes.NewReader(objData[startOffset:end])
	_, err = reader.WriteTo(writer)
	return err
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (x *xObjects) GetObjectInfo(
	ctx context.Context,
	bucket, object string,
	opts minio.ObjectOptions,
) (objInfo minio.ObjectInfo, err error) {
	if !x.ledgerStore.BucketExists(bucket) {
		return objInfo, x.toMinioErr(ErrLedgerBucketDoesNotExist, bucket, "", "")
	}
	info, err := x.getMinioObjectInfo(ctx, bucket, object)
	return info, x.toMinioErr(err, bucket, object, "")
}

// PutObject creates a new object with the incoming data,
func (x *xObjects) PutObject(
	ctx context.Context,
	bucket, object string,
	r *minio.PutObjReader,
	opts minio.ObjectOptions,
) (objInfo minio.ObjectInfo, err error) {
	if !x.ledgerStore.BucketExists(bucket) {
		return objInfo, x.toMinioErr(ErrLedgerBucketDoesNotExist, bucket, "", "")
	}
	// TODO(bonedaddy): ensure consistency with the way s3 and b2 handle this
	obinfo := ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: time.Now().UTC(),
	}
	for k, v := range opts.UserDefined {
		switch strings.ToLower(k) {
		case "content-encoding":
			obinfo.ContentEncoding = v
		case "content-disposition":
			obinfo.ContentDisposition = v
		case "content-language":
			obinfo.ContentLanguage = v
		case "content-type":
			obinfo.ContentType = v
		}
	}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return objInfo, x.toMinioErr(err, bucket, object, "")
	}
	obinfo.Size_ = int64(len(data))
	dataHash, err := x.dagPut(ctx, data)
	if err != nil {
		return objInfo, x.toMinioErr(err, bucket, object, "")
	}
	// add the object to ipfs
	objectHash, err := x.objectToIPFS(ctx, &Object{
		DataHash:   dataHash,
		ObjectInfo: obinfo,
	})
	if err != nil {
		return objInfo, x.toMinioErr(err, bucket, object, "")
	}
	// update the bucket on ipfs with the new object
	bucketHash, err := x.addObjectToBucketAndIPFS(ctx, object, objectHash, bucket)
	if err != nil {
		return objInfo, x.toMinioErr(err, bucket, object, "")
	}
	// update internal ledger state with bucket hash
	if err := x.ledgerStore.UpdateBucketHash(bucket, bucketHash); err != nil {
		return objInfo, x.toMinioErr(err, bucket, object, "")
	}
	// update internal ledger state with the new object
	if err := x.ledgerStore.AddObjectToBucket(bucket, object, objectHash); err != nil {
		return objInfo, x.toMinioErr(err, bucket, object, "")
	}
	log.Printf(
		"bucket-name: %s, bucket-hash: %s, object-name: %s, object-hash: %s",
		bucket, bucketHash, object, objectHash,
	)
	// convert the proto object into a minio.ObjectInfo type
	return x.getMinioObjectInfo(ctx, bucket, object)
}

// CopyObject copies an object from source bucket to a destination bucket.
func (x *xObjects) CopyObject(
	ctx context.Context,
	srcBucket string,
	srcObject string,
	dstBucket string,
	dstObject string,
	srcInfo minio.ObjectInfo,
	srcOpts, dstOpts minio.ObjectOptions,
) (objInfo minio.ObjectInfo, err error) {
	// TODO(bonedaddy): implement usage of options
	// TODO(bonedaddy): ensure we properly update the ledger with the destination object
	// TODO(bonedaddy): ensure the destination object is properly adjusted with metadata
	// ensure source bucket exists
	if !x.ledgerStore.BucketExists(srcBucket) {
		return objInfo, x.toMinioErr(ErrLedgerBucketDoesNotExist, srcBucket, "", "")
	}
	// ensure destination bucket exists
	if !x.ledgerStore.BucketExists(dstBucket) {
		return objInfo, x.toMinioErr(ErrLedgerBucketDoesNotExist, dstBucket, "", "")
	}
	// we need to update the object info to list the bucket it is in
	obj, err := x.objectFromBucket(ctx, srcBucket, srcObject)
	if err != nil {
		return objInfo, x.toMinioErr(err, srcBucket, srcObject, "")
	}
	// update relevant fields
	obj.ObjectInfo.Name = dstObject
	obj.ObjectInfo.Bucket = dstBucket
	obj.ObjectInfo.ModTime = time.Now().UTC()
	// store the updated bucket on ipfs
	dstObjHash, err := x.objectToIPFS(ctx, obj)
	if err != nil {
		return objInfo, x.toMinioErr(err, dstBucket, dstObject, "")
	}
	// update the destination bucket object on ipfs to the newly added object
	dstBucketHash, err := x.addObjectToBucketAndIPFS(ctx, dstObject, dstObjHash, dstBucket)
	if err != nil {
		return objInfo, x.toMinioErr(err, dstBucket, dstObject, "")
	}
	// now we need to update our ledger with the newly updated object for future lookups
	if err := x.ledgerStore.AddObjectToBucket(dstBucket, dstObject, dstObjHash); err != nil {
		return objInfo, x.toMinioErr(err, dstBucket, dstObject, "")
	}
	// then we must also update the newly updated bucket hash in the ledger as well
	if err := x.ledgerStore.UpdateBucketHash(dstBucket, dstBucketHash); err != nil {
		return objInfo, x.toMinioErr(err, dstBucket, dstObject, "")
	}
	log.Printf(
		"dst-bucket: %s, dst-bucket-hash: %s, dst-object: %s, dst-object-hash: %s\n",
		dstBucket, dstBucketHash, dstObject, dstObjHash,
	)
	objInfo, err = x.getMinioObjectInfo(ctx, dstBucket, dstObject)
	return objInfo, x.toMinioErr(err, dstBucket, dstObject, "")
}

// DeleteObject deletes a blob in bucket
func (x *xObjects) DeleteObject(
	ctx context.Context,
	bucket, object string,
) error {
	// this handles returning an error indicating whether
	// or not the bucket/object is not present
	if err := x.ledgerStore.ObjectExists(bucket, object); err != nil {
		return x.toMinioErr(err, bucket, object, "")
	}
	//TODO(bonedaddy): implement removal from IPFS
	err := x.ledgerStore.RemoveObject(bucket, object)
	return x.toMinioErr(err, bucket, object, "")
}

func (x *xObjects) DeleteObjects(
	ctx context.Context,
	bucket string,
	objects []string,
) ([]error, error) {
	if !x.ledgerStore.BucketExists(bucket) {
		return nil, x.toMinioErr(ErrLedgerBucketDoesNotExist, bucket, "", "")
	}
	// TODO(bonedaddy): implement removal from ipfs
	errs := make([]error, len(objects))
	for i, object := range objects {
		errs[i] = x.toMinioErr(
			x.ledgerStore.RemoveObject(bucket, object),
			bucket, object, "",
		)
	}
	return errs, nil
}
