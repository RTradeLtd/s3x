package s3x

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	minio "github.com/RTradeLtd/s3x/cmd"
	"github.com/RTradeLtd/s3x/pkg/auth"
)

const (
	testBucket1, testBucket2 = "bucket1", "testbucket2"
)

type testGateway struct {
	*xObjects
	testPath string
}

func (t *testGateway) Shutdown(ctx context.Context) error {
	os.RemoveAll(t.testPath)
	return t.xObjects.Shutdown(ctx)
}

var _ minio.ObjectLayer = &testGateway{}

// getTestGateway returns a testGateway that implements minio.ObjectLayer.
// testGateway also removes all data save on disk when shutdown
func getTestGateway(t *testing.T) *testGateway {
	testPath, err := ioutil.TempDir("", "s3x-test")
	if err != nil {
		t.Fatal(err)
	}
	os.Setenv("S3X_DS_PATH", testPath)
	defer os.Unsetenv("S3X_DS_PATH")
	temx := &TEMX{
		HTTPAddr: "localhost:8889",
		GRPCAddr: "localhost:8888",
		DSPath:   testPath,
		XAddr:    "xapi-dev.temporal.cloud:9090",
	}
	gateway, err := temx.NewGatewayLayer(auth.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	return &testGateway{
		xObjects: gateway.(*xObjects),
		testPath: testPath,
	}
}

func TestGateway_Bucket(t *testing.T) {
	//	testDial(t)
	gateway := getTestGateway(t)
	defer func() {
		if err := gateway.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
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
}
