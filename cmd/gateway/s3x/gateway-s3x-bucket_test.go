package s3x

import (
	"context"
	"os"
	"testing"
	"time"

	minio "github.com/RTradeLtd/s3x/cmd"
	"github.com/RTradeLtd/s3x/pkg/auth"
)

const (
	testBucket1, testBucket2 = "bucket1", "testbucket2"
)

func TestGateway_Bucket(t *testing.T) {
	testPath := "tmp-bucket-test"
	os.Setenv("S3X_DS_PATH", testPath)
	temx := &TEMX{
		HTTPAddr: "0.0.0.0:8889",
		GRPCAddr: "0.0.0.0:8888",
		DSPath:   "s3xstore",
		XAddr:    "xapi-dev.temporal.cloud:9090",
	}
	gateway, err := temx.NewGatewayLayer(auth.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	sinfo := gateway.StorageInfo(context.Background())
	if sinfo.Backend.Type != minio.BackendGateway {
		t.Fatal("bad type")
	}
	type args struct {
		bucketName, objectName, bucketHash, objectHash string
	}
	then := time.Now().UTC()
	t.Run("MakeBucketWithLocation", func(t *testing.T) {
		tests := []struct {
			name    string
			args    args
			wantErr bool
		}{
			{"Bucket1-Success", args{testBucket1, "", "", ""}, false},
			{"Bucket1-AlreadyExists", args{testBucket1, "", "", ""}, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := gateway.MakeBucketWithLocation(
					context.Background(),
					tt.args.bucketName,
					"us-east-1",
				)
				if (err != nil) != tt.wantErr {
					t.Fatalf("MakeBucketWithLocation() err %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})
	t.Run("Bucket Created Time Test", func(t *testing.T) {
		now := time.Now().UTC()
		info, err := gateway.GetBucketInfo(context.Background(), testBucket1)
		if err != nil {
			t.Fatal(err)
		}
		if info.Created.After(now) || info.Created.Before(then) {
			t.Fatal("bad bucket created time")
		}
	})
	t.Run("GetBucketInfo", func(t *testing.T) {
		tests := []struct {
			name    string
			args    args
			wantErr bool
		}{
			{"Bucket1-Found", args{testBucket1, "", "", ""}, false},
			{"Bucket2-NotFound", args{testBucket2, "", "", ""}, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				info, err := gateway.GetBucketInfo(context.Background(), tt.args.bucketName)
				if (err != nil) != tt.wantErr {
					t.Fatalf("GetBucketInfo() err %v, wantERr %v", err, tt.wantErr)
				}
				if err == nil && info.Name != tt.args.bucketName {
					t.Fatal("bad bucket name")
				}
			})
		}
	})
	t.Run("ListBuckets", func(t *testing.T) {
		var (
			wantNames = map[string]bool{
				testBucket1: true,
			}
			foundNames = map[string]bool{
				testBucket1: false,
			}
		)
		bucketInfos, err := gateway.ListBuckets(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, info := range bucketInfos {
			if wantNames[info.Name] {
				foundNames[info.Name] = true
			}
		}
		for name := range wantNames {
			if !foundNames[name] {
				t.Fatal("failed to find name")
			}
		}
	})
	t.Run("DeleteBucket", func(t *testing.T) {
		tests := []struct {
			name    string
			args    args
			wantErr bool
		}{
			{"Bucket1-Found", args{testBucket1, "", "", ""}, false},
			{"Bucket2-NotFound", args{testBucket2, "", "", ""}, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := gateway.DeleteBucket(context.Background(), tt.args.bucketName)
				if (err != nil) != tt.wantErr {
					t.Fatalf("DeleteBucket() err %v, wantErr %v", err, tt.wantErr)
				}
			})
		}
	})
	os.Unsetenv("S3X_DS_PATH")
	os.RemoveAll(testPath)
}
