package s3x

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	minio "github.com/RTradeLtd/s3x/cmd"
	"github.com/RTradeLtd/s3x/pkg/auth"
)

type testGateway struct {
	*xObjects
	testPath string
}

func (t *testGateway) Shutdown(ctx context.Context) error {
	err := t.xObjects.Shutdown(ctx)
	if err != nil {
		return err
	}
	return os.RemoveAll(t.testPath)
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
	g, err := temx.NewGatewayLayer(auth.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	return &testGateway{
		xObjects: g.(*xObjects),
		testPath: testPath,
	}
}
