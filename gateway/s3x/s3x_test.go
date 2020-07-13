package s3x

import (
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
)

func init() {
	isTest = true
}

func TestS3X_xObjects_GetHash_Badger(t *testing.T) {
	testS3XxObjectsGetHash(t, DSTypeBadger, false)
}
func TestS3X_xObjects_GetHash_Badger_Passthrough(t *testing.T) {
	testS3XxObjectsGetHash(t, DSTypeBadger, true)
}
func TestS3X_xObjects_GetHash_Crdt(t *testing.T) {
	testS3XxObjectsGetHash(t, DSTypeCrdt, false)
}
func TestS3X_xObjects_GetHash_Crdt_Passthrough(t *testing.T) {
	testS3XxObjectsGetHash(t, DSTypeCrdt, true)
}
func testS3XxObjectsGetHash(t *testing.T, dsType DSType, passthrough bool) {
	tests := []struct {
		name    string
		req     *InfoRequest
		want    *InfoResponse
		wantErr bool
	}{
		{"bucket hash", &InfoRequest{Bucket: testBucket1}, &InfoResponse{Bucket: testBucket1, Hash: "bafkreihi44b3qzbl6dahwyojtwrjkimefnuyrlazdvkw4ykhc4iugqaeue"}, false},
		{"InvalidArgument", &InfoRequest{Bucket: ""}, nil, true},
		{"object hash", &InfoRequest{Bucket: testBucket1, Object: testObject1}, &InfoResponse{Bucket: testBucket1, Object: testObject1, Hash: "bafkreielg2afdfwnjfr7hiz4drtd53tnyapepit6wun2etqyw6ab5suovq"}, false},
		{"object data hash", &InfoRequest{Bucket: testBucket1, Object: testObject1, ObjectDataOnly: true}, &InfoResponse{Bucket: testBucket1, Object: testObject1, Hash: "bafybeidespqxhoavxmrq6sxcypcwatb6u3splitarmw7z46pivdhahluaa"}, false},
	}

	ctx := context.Background()
	gateway := newTestGateway(t, dsType, passthrough)
	defer func() {
		if err := gateway.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	opts := minio.BucketOptions{
		Location: "us-east-1",
	}
	if err := gateway.MakeBucketWithLocation(ctx, testBucket1,opts); err != nil {
		t.Fatal(err)
	}
	testPutObject(t, gateway)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := gateway.GetHash(ctx, tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("xObjects.GetHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("xObjects.GetHash() = %v, want %v", got, tt.want)
			}
		})
	}
}

type testGateway struct {
	*xObjects
	temx     *TEMX
	testPath string
}

//restart restarts the gateway to simulate restarting the server during testing
func (g *testGateway) restart(t *testing.T) {
	err := g.xObjects.Shutdown(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := g.temx.NewGatewayLayer(auth.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	g.xObjects = gateway.(*xObjects)
}

func (g *testGateway) Shutdown(ctx context.Context) error {
	err := g.xObjects.Shutdown(ctx)
	if err != nil {
		return err
	}
	os.Unsetenv("S3X_DS_PATH")
	return os.RemoveAll(g.testPath)
}

var (
	_           minio.ObjectLayer = &testGateway{}
	pathOnce    sync.Once
	testPath    string
	testPathErr error
)

// newTestGateway returns a testGateway that implements minio.ObjectLayer.
// testGateway also removes all data save on disk when shutdown
func newTestGateway(t *testing.T, dsType DSType, passthrough bool) *testGateway {
	pathOnce.Do(func() {
		testPath, testPathErr = ioutil.TempDir("", "s3x-test")
	})
	if testPathErr != nil {
		t.Fatal(testPathErr)
	}
	if err := os.RemoveAll(testPath); err != nil { //clean up just to be sure
		t.Fatal(err)
	}

	xaddr := os.Getenv("TEST_XAPI")
	if xaddr == "" {
		xaddr = "xapi.temporal.cloud:9090"
	}
	os.Setenv("S3X_DS_PATH", testPath)
	temx := &TEMX{
		HTTPAddr:      "localhost:8889",
		GRPCAddr:      "localhost:8888",
		DSType:        dsType,
		DSPath:        testPath,
		DSPassthrough: passthrough,
		CrdtTopic:     testPath + time.Now().String(), //make sure the topic is unique
		XAddr:         xaddr,
		Insecure:      true,
	}
	g, err := temx.NewGatewayLayer(auth.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	return &testGateway{
		xObjects: g.(*xObjects),
		temx:     temx,
		testPath: testPath,
	}
}
