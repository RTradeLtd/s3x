package s3x

import (
	"context"
	"fmt"
	"testing"

	minio "github.com/minio/minio/cmd"
)

func TestS3XG_ListObjects_Badger(t *testing.T) {
	testS3XGListObjects(t, DSTypeBadger, false)
}
func TestS3XG_ListObjects_Passthrough(t *testing.T) {
	testS3XGListObjects(t, DSTypeBadger, true)
}
func TestS3XG_ListObjects_Crdt(t *testing.T) {
	testS3XGListObjects(t, DSTypeCrdt, false)
}
func TestS3XG_ListObjects_Crdt_Passthrough(t *testing.T) {
	testS3XGListObjects(t, DSTypeCrdt, true)
}
func testS3XGListObjects(t *testing.T, dsType DSType, passthrough bool) {
	ctx := context.Background()
	gateway := newTestGateway(t, dsType, passthrough)
	defer func() {
		if err := gateway.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	files := []string{
		"sample.jpg",
		"photos/2006/January/sample.jpg",
		"photos/2006/February/sample2.jpg",
		"photos/2006/February/sample3.jpg",
		"photos/2006/February/sample4.jpg",
	}

	type result struct {
		objects []string
		folders []string
	}

	prefixes := make(map[string]result)
	prefixes[""] = result{
		objects: []string{"sample.jpg"},
		folders: []string{"photos/"},
	}
	prefixes["photos/"] = result{
		folders: []string{"photos/2006/"},
	}
	prefixes["photos/2006/"] = result{
		folders: []string{
			"photos/2006/February/", //February comes before January by alphabetical ordering
			"photos/2006/January/",
		},
	}
	prefixes["photos/2006/February/"] = result{
		objects: []string{
			"photos/2006/February/sample2.jpg",
			"photos/2006/February/sample3.jpg",
			"photos/2006/February/sample4.jpg",
		},
	}

	if err := gateway.MakeBucketWithLocation(ctx, testBucket1, "us-east-1", false); err != nil {
		t.Fatal(err)
	}
	for _, fn := range files {
		if _, err := gateway.xObjects.PutObject(ctx, testBucket1, fn, getTestPutObjectReader(t, []byte(testObject1Data)), minio.ObjectOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	for prefix, exp := range prefixes {
		t.Run(fmt.Sprintf(`prefix="%v", delimiter="/"`, prefix), func(t *testing.T) {
			loi, err := gateway.xObjects.ListObjects(ctx, testBucket1, prefix, "", "/", 0)
			if err != nil {
				t.Fatal(err)
			}
			if loi.IsTruncated {
				t.Fatal("result should not be truncated")
			}
			if len(loi.Objects) != len(exp.objects) {
				t.Fatal("unexpected number of objects", loi)
			}
			for i, v := range loi.Objects {
				if v.Name != exp.objects[i] {
					t.Fatal("unexpected file name", v)
				}
			}
			for len(loi.Prefixes) != len(exp.folders) {
				t.Fatal("unexpected number of prefixes", loi)
			}
			for i, v := range loi.Prefixes {
				if v != exp.folders[i] {
					t.Fatal("unexpected prefix", v)
				}
			}
		})

		t.Run(fmt.Sprintf(`V2 prefix="%v", delimiter="/"`, prefix), func(t *testing.T) {
			loi, err := gateway.xObjects.ListObjectsV2(ctx, testBucket1, prefix, "", "/", 0, false, "")
			if err != nil {
				t.Fatal(err)
			}
			if loi.IsTruncated {
				t.Fatal("result should not be truncated")
			}
			if len(loi.Objects) != len(exp.objects) {
				t.Fatal("unexpected number of objects", loi)
			}
			for i, v := range loi.Objects {
				if v.Name != exp.objects[i] {
					t.Fatal("unexpected file name", v)
				}
			}
			for len(loi.Prefixes) != len(exp.folders) {
				t.Fatal("unexpected number of prefixes", loi)
			}
			for i, v := range loi.Prefixes {
				if v != exp.folders[i] {
					t.Fatal("unexpected prefix", v)
				}
			}
		})
	}
}
