package temx

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	pb "github.com/RTradeLtd/TxPB/go"
	minio "github.com/RTradeLtd/s3x/cmd"
	"github.com/RTradeLtd/s3x/pkg/auth"
	"github.com/RTradeLtd/s3x/pkg/policy"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/minio/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	temxBackend = "temx"
)

func init() {
	// TODO(bonedaddy): add help command
	minio.RegisterGatewayCommand(cli.Command{
		Name:   temxBackend,
		Usage:  "TemporalX IPFS",
		Action: temxGatewayMain,
	})
}
func temxGatewayMain(ctx *cli.Context) {
	minio.StartGateway(ctx, &TEMX{})
}

// TEMX implements MinIO Gateway
type TEMX struct{}

// NewGatewayLayer creates a minio gateway layer powered y TemporalX
func (g *TEMX) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	conn, err := grpc.Dial("xapi-dev.temporal.cloud:9090", grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})))
	if err != nil {
		return nil, err
	}
	return &xObjects{
		creds:     creds,
		dagClient: pb.NewDagAPIClient(conn),
		httpClient: &http.Client{
			Transport: minio.NewCustomHTTPTransport(),
		},
		ctx:         context.Background(),
		ledgerStore: newLedgerStore(dssync.MutexWrap(datastore.NewMapDatastore())),
	}, nil
}

// Name returns the name of the TemporalX gateway backend
func (g *TEMX) Name() string {
	return temxBackend
}

// Production indicates that this backend is suitable for production use
func (g *TEMX) Production() bool {
	return true
}

type xObjects struct {
	minio.GatewayUnsupported
	mu         sync.Mutex
	creds      auth.Credentials
	dagClient  pb.DagAPIClient
	httpClient *http.Client
	ctx        context.Context

	// ledgerStore is responsible for updating our internal ledger state
	ledgerStore *ledgerStore
}

func (x *xObjects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo is not relevant to TemporalX backend.
func (x *xObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo) {
	si.Backend.Type = minio.BackendGateway
	//si.Backend.GatewayOnline = minio.IsBackendOnline(ctx, x.httpClient, "https://docsx.temporal.cloud")
	return si
}

// MakeBucket creates a new bucket container within TemporalX.
func (x *xObjects) MakeBucketWithLocation(ctx context.Context, name, location string) error {
	buck := &Bucket{
		BucketInfo: &BucketInfo{
			Name:     name,
			Location: location,
			Created:  time.Now().UTC().String(),
		},
	}
	hash, err := x.bucketToIPFS(ctx, buck)
	if err != nil {
		return err
	}
	log.Printf("bucket-name: %s\tbucket-hash: %s", name, hash)
	// TODO(bonedaddy): check at start of call is bucket already exists
	err = x.ledgerStore.NewBucket(name, hash)
	if err != nil {
		switch err {
		case ErrLedgerBucketExists:
			err = minio.BucketAlreadyExists{Bucket: name}
		}
		return err
	}
	return nil
}

// GetBucketInfo gets bucket metadata..
func (x *xObjects) GetBucketInfo(ctx context.Context, name string) (bi minio.BucketInfo, err error) {
	bucket, err := x.bucketFromIPFS(ctx, name)
	if err != nil {
		return bi, err
	}
	created, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", bucket.GetBucketInfo().GetCreated())
	if err != nil {
		return bi, err
	}
	return minio.BucketInfo{
		Name: bucket.GetBucketInfo().GetName(),
		// should we do it like this?
		// Created: time.Unix(0, 0),
		Created: created,
	}, nil
}

// ListBuckets lists all S3 buckets
func (x *xObjects) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	names, err := x.ledgerStore.GetBucketNames()
	if err != nil {
		return nil, err
	}
	var infos = make([]minio.BucketInfo, len(names))
	for i, name := range names {
		info, err := x.GetBucketInfo(ctx, name)
		if err != nil {
			return nil, err
		}
		infos[i] = info
	}
	return infos, nil
}

// DeleteBucket deletes a bucket on S3
func (x *xObjects) DeleteBucket(ctx context.Context, name string) error {
	// TODO(bonedaddy): implement removal call from TemporalX
	// as of right now this just removes the bucket from our
	// internal ledger tracker
	return x.ledgerStore.DeleteBucket(name)
}

// ListObjects lists all blobs in S3 bucket filtered by prefix
func (x *xObjects) ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, e error) {
	// TODO(bonedaddy): implement
	/*	result, err := l.Client.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
		if err != nil {
			return loi, minio.ErrorRespToObjectError(err, bucket)
		}

		return minio.FromMinioClientListBucketResult(bucket, result), nil
	*/
	return loi, errors.New("not yet implemented")
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
func (x *xObjects) GetObject(ctx context.Context, bucket string, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	obj, err := x.objectFromBucket(ctx, bucket, object)
	if err != nil {
		return err
	}
	_, err = writer.Write(obj.GetData())
	return err
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (x *xObjects) GetObjectInfo(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return x.getMinioObjectInfo(ctx, bucket, object)
}

// PutObject creates a new object with the incoming data,
func (x *xObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	// TODO(bonedaddy): ensure consistency with the way s3 and b2 handle this
	obinfo := &ObjectInfo{}
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
	// TODO(bonedaddy): we probably need to implement a check here
	// that determines whether or not the bucket exists
	// get hash of th eobject
	objHash, err := x.ledgerStore.GetObjectHashFromBucket(srcBucket, srcObject)
	if err != nil {
		return objInfo, err
	}
	// update the destination bucket with the object hash under the destination object
	dstBucketHash, err := x.addObjectToBucketAndIPFS(ctx, dstObject, objHash, dstBucket)
	if err != nil {
		return objInfo, err
	}
	// update the internal ledger state for the destination bucket and destination bucket hash
	if err := x.ledgerStore.UpdateBucketHash(dstBucket, dstBucketHash); err != nil {
		return objInfo, err
	}
	return x.getMinioObjectInfo(ctx, dstBucket, dstObject)
}

// DeleteObject deletes a blob in bucket
func (x *xObjects) DeleteObject(ctx context.Context, bucket string, object string) error {
	//TODO(bonedaddy): implement
	/*
		bkt, err := l.Bucket(ctx, bucket)
		if err != nil {
			return err
		}

		// If we hide the file we'll conform to B2's versioning policy, it also
		// saves an additional call to check if the file exists first
		_, err = bkt.HideFile(l.ctx, object)
		logger.LogIf(ctx, err)
		return b2ToObjectError(err, bucket, object)
	*/
	return errors.New("not yet implemented")
}

func (x *xObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	//TODO(bonedaddy): implement
	return nil, errors.New("not yet implemented")
	/*	errs := make([]error, len(objects))
		for idx, object := range objects {
			errs[idx] = l.DeleteObject(ctx, bucket, object)
		}
		return errs, nil
	*/
}

// ListMultipartUploads lists all multipart uploads.
func (x *xObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, e error) {
	return lmi, errors.New("not yet implemented")
}

// NewMultipartUpload upload object in multiple parts
func (x *xObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, o minio.ObjectOptions) (uploadID string, err error) {
	return uploadID, errors.New("not yet implemented")
}

// PutObjectPart puts a part of object in bucket
func (x *xObjects) PutObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (pi minio.PartInfo, e error) {
	return pi, errors.New("not yet implemented")
}

// CopyObjectPart creates a part in a multipart upload by copying
// existing object or a part of it.
func (x *xObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string,
	partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	return p, errors.New("not yet implemented")
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (x *xObjects) ListObjectParts(ctx context.Context, bucket string, object string, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (lpi minio.ListPartsInfo, e error) {
	return lpi, errors.New("not yet implemented")
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (x *xObjects) AbortMultipartUpload(ctx context.Context, bucket string, object string, uploadID string) error {
	return errors.New("not yet implemented")
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (x *xObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (oi minio.ObjectInfo, e error) {
	return oi, errors.New("not yet implemented")
}

// SetBucketPolicy sets policy on bucket
func (x *xObjects) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	return errors.New("not yet implemented")
}

// GetBucketPolicy will get policy on bucket
func (x *xObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return nil, errors.New("not yet implemented")
}

// DeleteBucketPolicy deletes all policies on bucket
func (x *xObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return errors.New("not yet implemented")
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (x *xObjects) IsCompressionSupported() bool {
	return false
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (x *xObjects) IsEncryptionSupported() bool {
	return minio.GlobalKMS != nil || len(minio.GlobalGatewaySSE) > 0
}
