package s3x

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	minio "github.com/RTradeLtd/s3x/cmd"
	"github.com/RTradeLtd/s3x/pkg/auth"
)

func init() {
	isTest = true
}

type testGateway struct {
	*xObjects
	temx     *TEMX
	testPath string
}

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

var _ minio.ObjectLayer = &testGateway{}

// getTestGateway returns a testGateway that implements minio.ObjectLayer.
// testGateway also removes all data save on disk when shutdown
func getTestGateway(t *testing.T) *testGateway {
	testPath, err := ioutil.TempDir("", "s3x-test")
	if err != nil {
		t.Fatal(err)
	}
	os.Setenv("S3X_DS_PATH", testPath)
	temx := &TEMX{
		HTTPAddr: "localhost:8889",
		GRPCAddr: "localhost:8888",
		DSPath:   testPath,
		XAddr:    "xapi.temporal.cloud:9090",
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
