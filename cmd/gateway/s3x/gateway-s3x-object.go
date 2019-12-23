package s3x

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	minio "github.com/RTradeLtd/s3x/cmd"
)

// ListObjects lists all blobs in S3 bucket filtered by prefix
func (x *xObjects) ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, e error) {
	// TODO(bonedaddy): implement complex search
	if !x.ledgerStore.BucketExists(bucket) {
		return loi, minio.BucketNotFound{Bucket: bucket}
	}
	objHashes, err := x.ledgerStore.GetObjectHashes(bucket)
	if err != nil {
		return loi, x.toMinioErr(err, bucket, "")
	}
	loi.Objects = make([]minio.ObjectInfo, len(objHashes))
	var count int
	for name := range objHashes {
		info, err := x.getMinioObjectInfo(ctx, bucket, name)
		if err != nil {
			return loi, x.toMinioErr(err, bucket, name)
		}
		loi.Objects[count] = info
		count++
	}
	// TODO(bonedaddy): consider if we should use the following helper func
	// return minio.FromMinioClientListBucketResult(bucket, result), nil
	return loi, nil
}

// ListObjectsV2 lists all objects in B2 bucket filtered by prefix, returns upto max 1000 entries at a time.
func (x *xObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	// TODO(bonedaddy): implement
	/*
		// fetchOwner is not supported and unused.
		marker := continuationToken
		if marker == "" {
			// B2's continuation token is an object name to "start at" rather than "start after"
			// startAfter plus the lowest character B2 supports is used so that the startAfter
			// object isn't included in the results
			marker = startAfter + " "
		}

		bkt, err := l.Bucket(ctx, bucket)
		if err != nil {
			return loi, err
		}
		files, next, lerr := bkt.ListFileNames(l.ctx, maxKeys, marker, prefix, delimiter)
		if lerr != nil {
			logger.LogIf(ctx, lerr)
			return loi, b2ToObjectError(lerr, bucket)
		}
		loi.IsTruncated = next != ""
		loi.ContinuationToken = continuationToken
		loi.NextContinuationToken = next
		for _, file := range files {
			switch file.Status {
			case "folder":
				loi.Prefixes = append(loi.Prefixes, file.Name)
			case "upload":
				loi.Objects = append(loi.Objects, minio.ObjectInfo{
					Bucket:      bucket,
					Name:        file.Name,
					ModTime:     file.Timestamp,
					Size:        file.Size,
					ETag:        minio.ToS3ETag(file.ID),
					ContentType: file.Info.ContentType,
					UserDefined: file.Info.Info,
				})
			}
		}
		return loi, nil
	*/
	return loi, errors.New("not yet implemented")
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (x *xObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	// TODO(bonedaddy): implement
	/*
		var objInfo minio.ObjectInfo
		objInfo, err = l.GetObjectInfo(ctx, bucket, object, opts)
		if err != nil {
			return nil, err
		}

		var startOffset, length int64
		startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
		if err != nil {
			return nil, err
		}

		pr, pw := io.Pipe()
		go func() {
			err := l.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
			pw.CloseWithError(err)
		}()
		// Setup cleanup function to cause the above go-routine to
		// exit in case of partial read
		pipeCloser := func() { pr.Close() }
		return minio.NewGetObjectReaderFromReader(pr, objInfo, opts.CheckCopyPrecondFn, pipeCloser)
	*/
	return gr, errors.New("not yet implemented")
}

// GetObject reads an object from B2. Supports additional
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
	obj, err := x.objectFromBucket(ctx, bucket, object)
	if err != nil {
		return x.toMinioErr(err, bucket, object)
	}
	_, err = writer.Write(obj.GetData())
	return err
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (x *xObjects) GetObjectInfo(
	ctx context.Context,
	bucket, object string,
	opts minio.ObjectOptions,
) (objInfo minio.ObjectInfo, err error) {
	return x.getMinioObjectInfo(ctx, bucket, object)
}

// PutObject creates a new object with the incoming data,
func (x *xObjects) PutObject(
	ctx context.Context,
	bucket, object string,
	r *minio.PutObjReader,
	opts minio.ObjectOptions,
) (objInfo minio.ObjectInfo, err error) {
	// TODO(bonedaddy): ensure consistency with the way s3 and b2 handle this
	obinfo := ObjectInfo{}
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
	obinfo.ModTime = time.Now().UTC().String()
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return objInfo, err
	}
	// add the object to ipfs
	objectHash, err := x.objectToIPFS(ctx, &Object{
		Data:       data,
		ObjectInfo: obinfo,
	})
	if err != nil {
		return objInfo, err
	}
	// update the bucket on ipfs with the new object
	bucketHash, err := x.addObjectToBucketAndIPFS(ctx, object, objectHash, bucket)
	if err != nil {
		return objInfo, err
	}
	// update internal ledger state with bucket hash
	if err := x.ledgerStore.UpdateBucketHash(bucket, bucketHash); err != nil {
		return objInfo, err
	}
	// update internal ledger state with the new object
	if err := x.ledgerStore.AddObjectToBucket(bucket, object, objectHash); err != nil {
		return objInfo, err
	}
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

	// TODO(bonedaddy): we probably need to implement a check here
	// that determines whether or not the bucket exists
	// get hash of th eobject
	objHash, err := x.ledgerStore.GetObjectHash(srcBucket, srcObject)
	if err != nil {
		return objInfo, x.toMinioErr(err, srcBucket, srcObject)
	}
	// update the destination bucket with the object hash under the destination object
	dstBucketHash, err := x.addObjectToBucketAndIPFS(ctx, dstObject, objHash, dstBucket)
	if err != nil {
		return objInfo, x.toMinioErr(err, dstBucket, dstObject)
	}
	// update the internal ledger state for the destination bucket and destination bucket hash
	if err := x.ledgerStore.UpdateBucketHash(dstBucket, dstBucketHash); err != nil {
		return objInfo, x.toMinioErr(err, dstBucket, dstObject)
	}
	objInfo, err = x.getMinioObjectInfo(ctx, dstBucket, dstObject)
	return objInfo, x.toMinioErr(err, dstBucket, dstObject)
}

// DeleteObject deletes a blob in bucket
func (x *xObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	//TODO(bonedaddy): implement removal from IPFS
	err := x.ledgerStore.RemoveObject(bucket, object)
	switch err {
	case ErrLedgerBucketDoesNotExist:
		err = minio.BucketNotFound{Bucket: bucket}
	case ErrLedgerObjectDoesNotExist:
		err = minio.ObjectNotFound{Bucket: bucket, Object: object}
	}
	return x.toMinioErr(err, bucket, object)
}

func (x *xObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	// TODO(bonedaddy): implement removal from ipfs
	errs := make([]error, len(objects))
	for i, object := range objects {
		errs[i] = x.toMinioErr(
			x.ledgerStore.RemoveObject(bucket, object),
			bucket, object,
		)
	}
	return errs, nil
}
