package s3x

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/RTradeLtd/s3x/cmd"
	"github.com/RTradeLtd/s3x/pkg/auth"
	"github.com/RTradeLtd/s3x/pkg/hash"
)

const (
	testObject1     = "testobject1"
	testObject1Data = "testobject1data"
)

func TestGateway_Object(t *testing.T) {
	testDial(t)
	testPath := "tmp-bucket-test"
	defer func() {
		os.Unsetenv("S3X_DS_PATH")
		os.RemoveAll(testPath)
	}()
	os.Setenv("S3X_DS_PATH", testPath)
	temx := &TEMX{
		HTTPAddr: "0.0.0.0:8889",
		GRPCAddr: "0.0.0.0:8888",
		DSPath:   testPath,
		XAddr:    "xapi-dev.temporal.cloud:9090",
	}
	gateway, err := temx.NewGatewayLayer(auth.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	type args struct {
		bucketName, objectName string
	}
	// setup test bucket
	if err := gateway.MakeBucketWithLocation(
		context.Background(),
		testBucket1,
		"us-east-1",
	); err != nil {
		t.Fatal(err)
	}
	t.Run("PutObject", func(t *testing.T) {
		type args struct {
			bucketName, objectName, objectData string
		}
		tests := []struct {
			name    string
			args    args
			wantErr bool
		}{
			{"OK-Bucket-Exists", args{testBucket1, testObject1, testObject1Data}, false},
			{"Fail-No-Bucket", args{testBucket2, testObject1, testObject1Data}, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := gateway.PutObject(
					context.Background(),
					tt.args.bucketName,
					tt.args.objectName,
					cmd.NewPutObjReader(
						toObjectReader(
							t,
							strings.NewReader(tt.args.objectData),
							int64(len(testObject1Data)),
						),
						nil, nil,
					),
					cmd.ObjectOptions{},
				)
				if (err != nil) != tt.wantErr {
					t.Fatalf("PutObject() err %v, wantErr %v", err, tt.wantErr)
				}
				if err == nil && resp.Bucket != tt.args.bucketName {
					t.Fatal("bad bucket name")
				}
			})
		}
	})
	t.Run("ListObjects", func(t *testing.T) {
		tests := []struct {
			name    string
			args    args
			wantErr bool
		}{
			{"Fail-BucketNotExist", args{testBucket2, ""}, true},
			{"Success", args{testBucket1, ""}, false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if _, err := gateway.ListObjects(
					context.Background(),
					tt.args.bucketName,
					"", "", "",
					500,
				); (err != nil) != tt.wantErr {
					t.Fatalf("ListObjects() err %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})
	t.Run("GetObjectInfo", func(t *testing.T) {
		tests := []struct {
			name    string
			args    args
			wantErr bool
		}{
			{"Ok", args{testBucket1, testObject1}, false},
			{"Fail-Bad-Object", args{testBucket1, "notarealobj"}, true},
			{"Fail-Bad-Bucket", args{"notarealbucket", testObject1}, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				info, err := gateway.GetObjectInfo(
					context.Background(),
					tt.args.bucketName,
					tt.args.objectName,
					cmd.ObjectOptions{},
				)
				if (err != nil) != tt.wantErr {
					t.Fatalf("GetObjectInfo() err %v, wantErr %v", err, tt.wantErr)
				}
				if tt.wantErr {
					return
				}
				if info.Bucket != tt.args.bucketName {
					t.Fatal("bad bucket")
				}
				if info.Name != tt.args.objectName {
					t.Fatal("bad object")
				}
			})
		}

	})
	t.Run("ListObjectsV2", func(t *testing.T) {
		t.Skip()
	})
	t.Run("GetObjectNInfo", func(t *testing.T) {
		tests := []struct {
			name    string
			args    args
			wantErr bool
		}{
			{"Ok", args{testBucket1, testObject1}, false},
			{"Fail-Bad-Object", args{testBucket1, "notarealobj"}, true},
			{"Fail-Bad-Bucket", args{"notarealbucket", testObject1}, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := gateway.GetObjectNInfo(
					context.Background(),
					tt.args.bucketName,
					tt.args.objectName,
					&cmd.HTTPRangeSpec{},
					nil,
					0,
					cmd.ObjectOptions{},
				)
				if (err != nil) != tt.wantErr {
					t.Fatalf("GetObjectNInfo() err %v, wantErr %v", err, tt.wantErr)
				}
				if tt.wantErr {
					return
				}
				if resp.ObjInfo.Bucket != tt.args.bucketName {
					t.Fatal("bad bucket")
				}
				if resp.ObjInfo.Name != tt.args.objectName {
					t.Fatal("bad object")
				}
			})
		}
	})

	t.Run("CopyObject", func(t *testing.T) {
		t.Skip("TODO")
	})
	t.Run("DeleteObject", func(t *testing.T) {
		t.Skip("TODO")
	})
	t.Run("DeleteObjects", func(t *testing.T) {
		t.Skip("TODO")
	})
}

func toObjectReader(t *testing.T, input io.Reader, size int64) *hash.Reader {
	r, err := hash.NewReader(input, size, "", "", size, false)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
