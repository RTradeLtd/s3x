package s3x

import (
	"context"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
)

func init() {
	isTest = true
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
func newTestGateway(t *testing.T, dsType DSType) *testGateway {
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
		HTTPAddr:  "localhost:8889",
		GRPCAddr:  "localhost:8888",
		DSType:    dsType,
		DSPath:    testPath,
		CrdtTopic: testPath + time.Now().String(), //make sure the topic is unique
		XAddr:     xaddr,
		Insecure:  true,
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
