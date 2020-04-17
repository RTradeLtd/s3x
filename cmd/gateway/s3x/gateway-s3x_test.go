package s3x

import (
	"context"
	"reflect"
	"testing"
)

func TestS3X_xObjects_GetHash_Badger(t *testing.T) {
	testS3XxObjectsGetHash(t, DSTypeBadger)
}
func TestS3X_xObjects_GetHash_Crdt(t *testing.T) {
	testS3XxObjectsGetHash(t, DSTypeCrdt)
}
func testS3XxObjectsGetHash(t *testing.T, dsType DSType) {
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
	gateway := newTestGateway(t, dsType)
	defer func() {
		if err := gateway.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	if err := gateway.MakeBucketWithLocation(ctx, testBucket1, "us-east-1"); err != nil {
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
