package temx

import (
	"context"
	"testing"
	"time"

	minio "github.com/RTradeLtd/s3x/cmd"
	"github.com/RTradeLtd/s3x/pkg/auth"
)

func TestGateway(t *testing.T) {
	temx := &TEMX{}
	gateway, err := temx.NewGatewayLayer(auth.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	sinfo := gateway.StorageInfo(context.Background())
	if sinfo.Backend.Type != minio.BackendGateway {
		t.Fatal("bad type")
	}
	then := time.Now()
	if err := gateway.MakeBucketWithLocation(context.Background(), "testbucket", "us-east-1"); err != nil {
		t.Fatal(err)
	}
	info, err := gateway.GetBucketInfo(context.Background(), "testbucket")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(time.Minute)
	if info.Created.After(now) || info.Created.Before(then) {
		t.Fatal("bad bucket time")
	}
}
