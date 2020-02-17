package s3x

import (
	"bytes"
	"context"
	"testing"

	minio "github.com/RTradeLtd/s3x/cmd"
)

func TestS3XGateway_Multipart(t *testing.T) {
	bucket := "my multipart bucket"
	object := "my multipart object"
	objectETag := "bafybeibzfoslocl3zs4fngsqminlpikibos7u664circ6mw7kjwkwa6y54"
	ctx := context.Background()
	gateway := getTestGateway(t)
	defer func() {
		if err := gateway.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	if err := gateway.MakeBucketWithLocation(ctx, bucket, "us-east-1"); err != nil {
		t.Fatal(err)
	}

	uID, err := gateway.NewMultipartUpload(ctx, bucket, object, minio.ObjectOptions{})
	t.Run("new multipart upload", func(t *testing.T) {
		if err != nil {
			t.Fatal(err)
		}
	})

	partData := []byte("data")
	parts := 6
	totalSize := len(partData) * parts
	partsInfo := []minio.PartInfo{}
	t.Run("add parts", func(t *testing.T) {
		for i := 0; i < parts; i++ {
			pi, err := gateway.PutObjectPart(ctx, bucket, object, uID, i, getTestPutObjectReader(t, partData), minio.ObjectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if pi.PartNumber != i {
				t.Fatalf("expected part number %v, but received %v", i, pi.PartNumber)
			}
			if pi.ETag != objectETag {
				t.Fatalf("expected ETag %v, but received %v", objectETag, pi.ETag)
			}
			partsInfo = append(partsInfo, pi)
		}
	})

	t.Run("complete", func(t *testing.T) {
		uploadParts := make([]minio.CompletePart, 0, parts)
		for _, pi := range partsInfo {
			uploadParts = append(uploadParts, minio.CompletePart{
				PartNumber: pi.PartNumber,
				ETag:       pi.ETag,
			})
		}
		oi, err := gateway.CompleteMultipartUpload(ctx, bucket, object, uID, uploadParts, minio.ObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if oi.Size != int64(totalSize) {
			t.Fatalf("expected file size %v, but received %v", totalSize, oi.Size)
		}
	})

	t.Run("get completed object", func(t *testing.T) {
		w := bytes.NewBuffer(nil)
		if err := gateway.GetObject(ctx, bucket, object, 0, 0, w, "", minio.ObjectOptions{}); err != nil {
			t.Fatal(err)
		}
		out := w.Bytes()
		if len(out) != totalSize {
			t.Fatalf("expected file size %v, but received %s", totalSize, out)
		}
	})
}
