package s3x

import (
	"context"
	"os"
	"testing"

	"github.com/RTradeLtd/s3x/pkg/auth"
)

const (
	testObject1     = "testobject1"
	testObject1Data = "testobject1data"
)

func TestGateway_Object(t *testing.T) {
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
	t.Run("ListObjectsV2", func(t *testing.T) {
		if _, err := gateway.ListObjectsV2(
			context.Background(),
			testBucket1, "", "", "",
			1000,
			true,
			"",
		); err == nil {
			t.Fatal("error expected")
		}
	})
}
